/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 */

#include "src/chunkserver/copyset_node.h"

#include <braft/closure_helper.h>
#include <braft/protobuf_file.h>
#include <braft/snapshot.h>
#include <brpc/controller.h>
#include <butil/sys_byteorder.h>
#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <memory>
#include <set>
#include <utility>

#include "src/chunkserver/chunk_closure.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/datastore/datastore_file_helper.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/op_request.h"
#include "src/chunkserver/raftsnapshot/curve_filesystem_adaptor.h"
#include "src/common/concurrent/task_thread_pool.h"
#include "src/common/crc32.h"
#include "src/common/fs_util.h"
#include "src/common/uri_parser.h"
#include "src/fs/fs_common.h"

namespace braft {
DECLARE_bool(raft_enable_leader_lease);
}  // namespace braft

namespace curve {
namespace chunkserver {

using curve::fs::FileSystemInfo;

const char* kCurveConfEpochFilename = "conf.epoch";

uint32_t CopysetNode::syncTriggerSeconds_ = 25;
std::shared_ptr<common::TaskThreadPool<>> CopysetNode::copysetSyncPool_ =
    nullptr;

CopysetNode::CopysetNode(const LogicPoolID& logicPoolId,
                         const CopysetID& copysetId,
                         const Configuration& initConf)
    : logicPoolId_(logicPoolId),
      copysetId_(copysetId),
      conf_(initConf),
      epoch_(0),
      peerId_(),
      nodeOptions_(),
      raftNode_(nullptr),
      chunkDataApath_(),
      chunkDataRpath_(),
      appliedIndex_(0),
      leaderTerm_(-1),
      configChange_(std::make_shared<ConfigurationChange>()),
      lastSnapshotIndex_(0),
      scaning_(false),
      lastScanSec_(0),
      enableOdsyncWhenOpenChunkFile_(false),
      isSyncing_(false),
      checkSyncingIntervalMs_(500) {}

CopysetNode::~CopysetNode() {
    // Remove metric from copyset
    ChunkServerMetric::GetInstance()->RemoveCopysetMetric(logicPoolId_,
                                                          copysetId_);
    metric_ = nullptr;

    if (nodeOptions_.snapshot_file_system_adaptor != nullptr) {
        delete nodeOptions_.snapshot_file_system_adaptor;
        nodeOptions_.snapshot_file_system_adaptor = nullptr;
    }
    LOG(INFO) << "release copyset node: " << GroupIdString();
}

int CopysetNode::Init(const CopysetNodeOptions& options) {
    std::string groupId = GroupId();

    std::string protocol = curve::common::UriParser::ParseUri(
        options.chunkDataUri, &copysetDirPath_);
    if (protocol.empty()) {
        // TODO(wudemiao): Add necessary error codes and return
        LOG(ERROR) << "not support chunk data uri's protocol"
                   << " error chunkDataDir is: " << options.chunkDataUri
                   << ". Copyset: " << GroupIdString();
        return -1;
    }

    /**
     * Init copyset node config related to chunk server, the
     * following two inits should be ahead of raftNode_.init
     */
    chunkDataApath_.append(copysetDirPath_).append("/").append(groupId);
    copysetDirPath_.append("/").append(groupId);
    fs_ = options.localFileSystem;
    CHECK(nullptr != fs_) << "local file sytem is null";
    epochFile_.reset(new ConfEpochFile(fs_));

    chunkDataRpath_ = RAFT_DATA_DIR;
    chunkDataApath_.append("/").append(RAFT_DATA_DIR);
    DataStoreOptions dsOptions;
    dsOptions.baseDir = chunkDataApath_;
    dsOptions.chunkSize = options.maxChunkSize;
    dsOptions.metaPageSize = options.metaPageSize;
    dsOptions.blockSize = options.blockSize;
    dsOptions.locationLimit = options.locationLimit;
    dsOptions.enableOdsyncWhenOpenChunkFile =
        options.enableOdsyncWhenOpenChunkFile;
    dataStore_ = std::make_shared<CSDataStore>(
        options.localFileSystem, options.chunkFilePool, dsOptions);
    CHECK(nullptr != dataStore_);
    if (false == dataStore_->Initialize()) {
        // TODO(wudemiao): Add necessary error codes and return
        LOG(ERROR) << "data store init failed. "
                   << "Copyset: " << GroupIdString();
        return -1;
    }
    enableOdsyncWhenOpenChunkFile_ = options.enableOdsyncWhenOpenChunkFile;
    if (!enableOdsyncWhenOpenChunkFile_) {
        syncThread_.Init(this);
        dataStore_->SetCacheCondPtr(syncThread_.cond_);
        dataStore_->SetCacheLimits(options.syncChunkLimit,
                                   options.syncThreshold);
        LOG(INFO) << "init sync thread success limit = "
                  << options.syncChunkLimit
                  << "syncthreshold = " << options.syncThreshold;
    }

    recyclerUri_ = options.recyclerUri;

    // init braft lease
    if (options.enbaleLeaseRead) {
        braft::FLAGS_raft_enable_leader_lease = true;
    }

    // initialize raft node options corresponding to the copy set node
    InitRaftNodeOptions(options);

    /* Initialize peer id */
    butil::ip_t ip;
    butil::str2ip(options.ip.c_str(), &ip);
    butil::EndPoint addr(ip, options.port);
    /**
     * The default idx is zero, and chunkserver does not allow a process to have
     * multiple copies of the same copyset, Pay attention to this point and not
     * distinguish between braces
     */
    peerId_ = PeerId(addr, 0);
    raftNode_ = std::make_shared<RaftNode>(groupId, peerId_);
    concurrentapply_ = options.concurrentapply;

    /*
     * Initialize copyset performance metrics
     */
    int ret = ChunkServerMetric::GetInstance()->CreateCopysetMetric(
        logicPoolId_, copysetId_);
    if (ret != 0) {
        LOG(ERROR) << "Create copyset metric failed. "
                   << "Copyset: " << GroupIdString();
        return -1;
    }
    metric_ = ChunkServerMetric::GetInstance()->GetCopysetMetric(logicPoolId_,
                                                                 copysetId_);
    if (metric_ != nullptr) {
        // TODO(yyk) will consider adding io metrics at the datastore level in
        // the future
        metric_->MonitorDataStore(dataStore_.get());
    }

    auto monitorMetricCb = [this](CurveSegmentLogStorage* logStorage) {
        logStorage_ = logStorage;
        metric_->MonitorCurveSegmentLogStorage(logStorage);
    };

    LogStorageOptions lsOptions(options.walFilePool, monitorMetricCb);

    // In order to get more copysetNode's information in CurveSegmentLogStorage
    // without using global variables.
    StoreOptForCurveSegmentLogStorage(lsOptions);

    checkSyncingIntervalMs_ = options.checkSyncingIntervalMs;

    return 0;
}

int CopysetNode::Run() {
    // The initialization of the raft node actually starts running
    if (0 != raftNode_->init(nodeOptions_)) {
        LOG(ERROR) << "Fail to init raft node. "
                   << "Copyset: " << GroupIdString();
        return -1;
    }

    if (!enableOdsyncWhenOpenChunkFile_) {
        syncThread_.Run();
    }

    LOG(INFO) << "Run copyset success."
              << "Copyset: " << GroupIdString();
    return 0;
}

void CopysetNode::Fini() {
    if (!enableOdsyncWhenOpenChunkFile_) {
        syncThread_.Stop();
    }

    WaitSnapshotDone();

    if (nullptr != raftNode_) {
        // Close all services related to this raft node
        raftNode_->shutdown(nullptr);
        // Waiting for all tasks being processed to end
        raftNode_->join();
    }
    if (nullptr != concurrentapply_) {
        // Drop the data that has not been flushed onto the disk, if not flushed
        // When migrating a copyset, removing the copyset before executing the
        // WriteChunk operation may result in errors
        concurrentapply_->Flush();
    }
}

void CopysetNode::InitRaftNodeOptions(const CopysetNodeOptions& options) {
    auto groupId = GroupId();
    nodeOptions_.initial_conf = conf_;
    nodeOptions_.election_timeout_ms = options.electionTimeoutMs;
    nodeOptions_.fsm = this;
    nodeOptions_.node_owns_fsm = false;
    nodeOptions_.snapshot_interval_s = options.snapshotIntervalS;
    nodeOptions_.log_uri = options.logUri;
    nodeOptions_.log_uri.append("/").append(groupId).append("/").append(
        RAFT_LOG_DIR);
    nodeOptions_.raft_meta_uri = options.raftMetaUri;
    nodeOptions_.raft_meta_uri.append("/").append(groupId).append("/").append(
        RAFT_META_DIR);
    nodeOptions_.snapshot_uri = options.raftSnapshotUri;
    nodeOptions_.snapshot_uri.append("/").append(groupId).append("/").append(
        RAFT_SNAP_DIR);
    nodeOptions_.usercode_in_pthread = options.usercodeInPthread;
    nodeOptions_.snapshot_throttle = options.snapshotThrottle;

    CurveFilesystemAdaptor* cfa = new CurveFilesystemAdaptor(
        options.chunkFilePool, options.localFileSystem);
    std::vector<std::string> filterList;
    std::string snapshotMeta(BRAFT_SNAPSHOT_META_FILE);
    filterList.push_back(kCurveConfEpochFilename);
    filterList.push_back(snapshotMeta);
    filterList.push_back(snapshotMeta.append(BRAFT_PROTOBUF_FILE_TEMP));
    cfa->SetFilterList(filterList);

    nodeOptions_.snapshot_file_system_adaptor =
        new scoped_refptr<braft::FileSystemAdaptor>(cfa);
}

void CopysetNode::on_apply(::braft::Iterator& iter) {
    for (; iter.valid(); iter.next()) {
        // Asynchronous execution in bthread to avoid blocking the execution of
        // the current state machine
        braft::AsyncClosureGuard doneGuard(iter.done());

        /**
         * Obtain the ChunkClosure passed when submitting tasks to Braft, which
         * includes All Contextual ChunkOpRequest for Op
         */
        braft::Closure* closure = iter.done();

        if (nullptr != closure) {
            /**
             * 1. If the closure is not null, it indicates that the current node
             * is normal and Op is directly obtained from memory Apply in
             * context
             */
            ChunkClosure* chunkClosure =
                dynamic_cast<ChunkClosure*>(iter.done());
            CHECK(nullptr != chunkClosure)
                << "ChunkClosure dynamic cast failed";
            std::shared_ptr<ChunkOpRequest>& opRequest = chunkClosure->request_;
            concurrentapply_->Push(
                opRequest->ChunkId(),
                ChunkOpRequest::Schedule(opRequest->OpType()),  // NOLINT
                &ChunkOpRequest::OnApply, opRequest, iter.index(),
                doneGuard.release());
        } else {
            // Obtain log entry
            butil::IOBuf log = iter.data();
            /**
             * 2. If the closure is null, there are two situations:
             * 2.1. Restart the node and replay the application. Here, the Op
             *      log entry will be deserialized, Then obtain Op information
             *      for application
             * 2.2. follower apply
             */
            ChunkRequest request;
            butil::IOBuf data;
            auto opReq = ChunkOpRequest::Decode(log, &request, &data,
                                                iter.index(), GetLeaderId());
            auto chunkId = request.chunkid();
            concurrentapply_->Push(
                chunkId, ChunkOpRequest::Schedule(request.optype()),  // NOLINT
                &ChunkOpRequest::OnApplyFromLog, opReq, dataStore_,
                std::move(request), data);
        }
    }
}

void CopysetNode::on_shutdown() {
    LOG(INFO) << GroupIdString() << " is shutdown";
}

void CopysetNode::on_snapshot_save(::braft::SnapshotWriter* writer,
                                   ::braft::Closure* done) {
    snapshotFuture_ =
        std::async(std::launch::async, &CopysetNode::save_snapshot_background,
                   this, writer, done);
}

void CopysetNode::WaitSnapshotDone() {
    // wait async snapshot done
    if (snapshotFuture_.valid()) {
        snapshotFuture_.wait();
    }
}

void CopysetNode::save_snapshot_background(::braft::SnapshotWriter* writer,
                                           ::braft::Closure* done) {
    brpc::ClosureGuard doneGuard(done);

    /**
     * 1. flush I/O to disk to ensure that all data is dropped
     */
    concurrentapply_->Flush();

    if (!enableOdsyncWhenOpenChunkFile_) {
        ForceSyncAllChunks();
    }

    /**
     * 2. Save the configuration version: conf.epoch, please note that
     * conf.epoch is stored in the data directory
     */
    std::string filePathTemp =
        writer->get_path() + "/" + kCurveConfEpochFilename;
    if (0 != SaveConfEpoch(filePathTemp)) {
        done->status().set_error(errno, "invalid: %s", strerror(errno));
        LOG(ERROR) << "SaveConfEpoch failed. "
                   << "Copyset: " << GroupIdString() << ", errno: " << errno
                   << ", "
                   << ", error message: " << strerror(errno);
        return;
    }

    /**
     * 3. Save the list of chunk file names to the snapshot metadata file
     */
    std::vector<std::string> files;
    if (0 == fs_->List(chunkDataApath_, &files)) {
        for (const auto& fileName : files) {
            // When saving a snapshot in the raft, there is no need to save the
            // list of snapshot files in the meta information.
            // When raft downloads a snapshot, after downloading the chunk,
            // a separate snapshot list will be obtained.
            bool isSnapshot = DatastoreFileHelper::IsSnapshotFile(fileName);
            if (isSnapshot) {
                continue;
            }
            std::string chunkApath;
            // Calculate the path relative to the snapshot directory through
            // absolute path
            chunkApath.append(chunkDataApath_);
            chunkApath.append("/").append(fileName);
            std::string filePath =
                curve::common::CalcRelativePath(writer->get_path(), chunkApath);
            writer->add_file(filePath);
        }
    } else {
        done->status().set_error(errno, "invalid: %s", strerror(errno));
        LOG(ERROR) << "dir reader failed, maybe no exist or permission. "
                   << "Copyset: " << GroupIdString()
                   << ", path: " << chunkDataApath_;
        return;
    }

    /**
     * 4. Save the conf.epoch file to the snapshot metadata file
     */
    writer->add_file(kCurveConfEpochFilename);
}

int CopysetNode::on_snapshot_load(::braft::SnapshotReader* reader) {
    /**
     * 1. Loading snapshot data
     */
    // Open snapshot path: /mnt/sda/1-10001/raft_snapshot/snapshot_0043
    std::string snapshotPath = reader->get_path();

    // /mnt/sda/1-10001/raft_snapshot/snapshot_0043/data
    std::string snapshotChunkDataDir;
    snapshotChunkDataDir.append(snapshotPath);
    snapshotChunkDataDir.append("/").append(chunkDataRpath_);
    LOG(INFO) << "load snapshot data path: " << snapshotChunkDataDir
              << ", Copyset: " << GroupIdString();
    // If the data directory does not exist, then the load snapshot data section
    // does not need to be processed
    if (fs_->DirExists(snapshotChunkDataDir)) {
        // Before loading snapshot data, clean the files in the copyset data
        // directory first Otherwise, it may result in some residual data after
        // the snapshot is loaded.
        // If delete_file or rename fails, the current node status will be set
        // to ERROR.
        // If delete_file or during the renamethe process restarts, and
        // after copyset is set, the snapshot will be loaded Since rename
        // ensures atomicity, after loading the snapshot, the data directory
        // must be restored.
        bool ret =
            nodeOptions_.snapshot_file_system_adaptor->get()->delete_file(
                chunkDataApath_, true);
        if (!ret) {
            LOG(ERROR) << "delete chunk data dir failed. "
                       << "Copyset: " << GroupIdString()
                       << ", path: " << chunkDataApath_;
            return -1;
        }
        LOG(INFO) << "delete chunk data dir success. "
                  << "Copyset: " << GroupIdString()
                  << ", path: " << chunkDataApath_;
        ret = nodeOptions_.snapshot_file_system_adaptor->get()->rename(
            snapshotChunkDataDir, chunkDataApath_);
        if (!ret) {
            LOG(ERROR) << "rename snapshot data dir " << snapshotChunkDataDir
                       << "to chunk data dir " << chunkDataApath_ << " failed. "
                       << "Copyset: " << GroupIdString();
            return -1;
        }
        LOG(INFO) << "rename snapshot data dir " << snapshotChunkDataDir
                  << "to chunk data dir " << chunkDataApath_ << " success. "
                  << "Copyset: " << GroupIdString();
    } else {
        LOG(INFO) << "load snapshot data path: " << snapshotChunkDataDir
                  << " not exist. "
                  << "Copyset: " << GroupIdString();
    }

    /**
     * 2. Load Configuration Version File
     */
    std::string filePath = reader->get_path() + "/" + kCurveConfEpochFilename;
    if (fs_->FileExists(filePath)) {
        if (0 != LoadConfEpoch(filePath)) {
            LOG(ERROR) << "load conf.epoch failed. "
                       << "path:" << filePath
                       << ", Copyset: " << GroupIdString();
            return -1;
        }
    }

    /**
     * 3. Reinitializing the data store, with examples:
     *
     * (1) For instance, when adding a new peer and immediately reading data,
     * the data store may return "chunk not exist." This is because the newly
     * added peer initially has no data, and when the data store is initialized,
     * it is not aware of the data that the new peer receives after the leader
     * recovers its data.
     *
     * (2) When a peer recovers all of its data through an install snapshot
     * operation, it is performed through a rename operation. If a file was
     * previously open in the data store, the rename operation can succeed, but
     * the old file can only be deleted after the data store closes it.
     * Therefore, it is necessary to reinitialize the data store, close the
     * file's file descriptor (fd), and then reopen the new file. Otherwise, the
     * data store will continue to operate on the old file. Once the data store
     * closes, the corresponding fd, any subsequent write operations will be
     * lost. Additionally, if the datastore is not reinitialized and the new
     * file is not reopened, it may result in reading the old data rather than
     * the recovered data.
     */
    if (!dataStore_->Initialize()) {
        LOG(ERROR) << "data store init failed in on snapshot load. "
                   << "Copyset: " << GroupIdString();
        return -1;
    }

    /**
     * 4. If conf is stored in the snapshot, load initialization to ensure that
     *    there is no need for on_configuration_committed. It should be noted
     *    that the log of the joint stage will be ignored here.
     */
    braft::SnapshotMeta meta;
    reader->load_meta(&meta);
    if (0 == meta.old_peers_size()) {
        conf_.reset();
        for (int i = 0; i < meta.peers_size(); ++i) {
            conf_.add_peer(meta.peers(i));
        }
    }

    LOG(INFO) << "update lastSnapshotIndex_ from " << lastSnapshotIndex_;
    lastSnapshotIndex_ = meta.last_included_index();
    LOG(INFO) << "to lastSnapshotIndex_: " << lastSnapshotIndex_;
    return 0;
}

void CopysetNode::on_leader_start(int64_t term) {
    /*
     * Invoke order in on_leader_start:
     *   1. flush concurrent apply queue.
     *   2. set term in states machine.
     *
     * Why is this order?
     *   If we want to invoke local read (lease read)
     *   in the leader state machine,
     *   states machine which raft leader node located must be newest.
     *   However, some tasks will accumulate in concurrent apply queue,
     *   so we must flush it before we enable local read.
     *
     * Connotation of order:
     *   If we set `leaderTerm_` in `on_leader_start`,
     *   it means that we will enable local read in current node.
     *
     * Corner case can reference by following pr:
     *   https://github.com/opencurve/curve/pull/2448
     */
    ChunkServerMetric::GetInstance()->IncreaseLeaderCount();
    concurrentapply_->Flush();
    leaderTerm_.store(term, std::memory_order_release);
    LOG(INFO) << "Copyset: " << GroupIdString()
              << ", peer id: " << peerId_.to_string()
              << " become leader, term is: " << leaderTerm_;
}

void CopysetNode::on_leader_stop(const butil::Status& status) {
    (void)status;
    leaderTerm_.store(-1, std::memory_order_release);
    ChunkServerMetric::GetInstance()->DecreaseLeaderCount();
    LOG(INFO) << "Copyset: " << GroupIdString()
              << ", peer id: " << peerId_.to_string() << " stepped down";
}

void CopysetNode::on_error(const ::braft::Error& e) {
    LOG(FATAL) << "Copyset: " << GroupIdString()
               << ", peer id: " << peerId_.to_string()
               << " meet raft error: " << e;
}

void CopysetNode::on_configuration_committed(const Configuration& conf,
                                             int64_t index) {
    // This function is also called when loading snapshot.
    // Loading snapshot should not increase epoch. When loading
    // snapshot, the index is equal with lastSnapshotIndex_.
    LOG(INFO) << "index: " << index
              << ", lastSnapshotIndex_: " << lastSnapshotIndex_;
    if (index != lastSnapshotIndex_) {
        std::unique_lock<std::mutex> lock_guard(confLock_);
        conf_ = conf;
        epoch_.fetch_add(1, std::memory_order_acq_rel);
    }
    LOG(INFO) << "Copyset: " << GroupIdString()
              << ", peer id: " << peerId_.to_string()
              << ", leader id: " << raftNode_->leader_id()
              << ", Configuration of this group is" << conf
              << ", epoch: " << epoch_.load(std::memory_order_acquire);
}

void CopysetNode::on_stop_following(const ::braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "Copyset: " << GroupIdString()
              << ", peer id: " << peerId_.to_string() << " stops following"
              << ctx;
}

void CopysetNode::on_start_following(const ::braft::LeaderChangeContext& ctx) {
    LOG(INFO) << "Copyset: " << GroupIdString()
              << ", peer id: " << peerId_.to_string() << "start following"
              << ctx;
}

LogicPoolID CopysetNode::GetLogicPoolId() const { return logicPoolId_; }

CopysetID CopysetNode::GetCopysetId() const { return copysetId_; }

void CopysetNode::SetScan(bool scan) { scaning_ = scan; }

bool CopysetNode::GetScan() const { return scaning_; }

void CopysetNode::SetLastScan(uint64_t time) { lastScanSec_ = time; }

uint64_t CopysetNode::GetLastScan() const { return lastScanSec_; }

std::vector<ScanMap>& CopysetNode::GetFailedScanMap() {
    return failedScanMaps_;
}

std::string CopysetNode::GetCopysetDir() const { return copysetDirPath_; }

uint64_t CopysetNode::GetConfEpoch() const {
    std::lock_guard<std::mutex> lockguard(confLock_);
    return epoch_.load(std::memory_order_relaxed);
}

int CopysetNode::LoadConfEpoch(const std::string& filePath) {
    LogicPoolID loadLogicPoolID = 0;
    CopysetID loadCopysetID = 0;
    uint64_t loadEpoch = 0;

    int ret = epochFile_->Load(filePath, &loadLogicPoolID, &loadCopysetID,
                               &loadEpoch);
    if (0 == ret) {
        if (logicPoolId_ != loadLogicPoolID || copysetId_ != loadCopysetID) {
            LOG(ERROR) << "logic pool id or copyset id not fit, "
                       << "(" << logicPoolId_ << "," << copysetId_ << ")"
                       << "not same with conf.epoch file: "
                       << "(" << loadLogicPoolID << "," << loadCopysetID << ")"
                       << ", Copyset: " << GroupIdString();
            ret = -1;
        } else {
            epoch_.store(loadEpoch, std::memory_order_relaxed);
        }
    }

    return ret;
}

int CopysetNode::SaveConfEpoch(const std::string& filePath) {
    return epochFile_->Save(filePath, logicPoolId_, copysetId_, epoch_);
}

void CopysetNode::ListPeers(std::vector<Peer>* peers) {
    std::vector<PeerId> tempPeers;

    {
        std::lock_guard<std::mutex> lockguard(confLock_);
        conf_.list_peers(&tempPeers);
    }

    for (auto it = tempPeers.begin(); it != tempPeers.end(); ++it) {
        Peer peer;
        peer.set_address(it->to_string());
        peers->push_back(peer);
    }
}

void CopysetNode::SetCSDateStore(std::shared_ptr<CSDataStore> datastore) {
    dataStore_ = datastore;
}

void CopysetNode::SetLocalFileSystem(std::shared_ptr<LocalFileSystem> fs) {
    fs_ = fs;
}

void CopysetNode::SetConfEpochFile(std::unique_ptr<ConfEpochFile> epochFile) {
    epochFile_ = std::move(epochFile);
}

void CopysetNode::SetCopysetNode(std::shared_ptr<RaftNode> node) {
    raftNode_ = node;
}

void CopysetNode::SetSnapshotFileSystem(scoped_refptr<FileSystemAdaptor>* fs) {
    nodeOptions_.snapshot_file_system_adaptor = fs;
}

bool CopysetNode::IsLeaderTerm() const {
    if (0 < leaderTerm_.load(std::memory_order_acquire)) return true;
    return false;
}

bool CopysetNode::IsLeaseLeader(
    const braft::LeaderLeaseStatus& lease_status) const {  // NOLINT
    /*
     * Why not use lease_status.state==LEASE_VALID directly to judge?
     *
     * Because of the existence of concurrent apply queue,
     * maybe there are some read/write requests in apply queue,
     * so the FSM is not up-to-date.
     * If we local read from this FSM,
     * we will meet some stale read problems.
     *
     * According to aforementioned reasons,
     * we judge leader lease when FSM had invoke on_leader_start,
     * and flush the concurrent apply queue.
     */
    auto term = leaderTerm_.load(std::memory_order_acquire);
    // if term == lease_status.term, the lease status must be LEASE_VALID
    return term > 0 && term == lease_status.term;
}

bool CopysetNode::IsLeaseExpired(
    const braft::LeaderLeaseStatus& lease_status) const {  // NOLINT
    return lease_status.state == braft::LEASE_EXPIRED;
}

PeerId CopysetNode::GetLeaderId() const { return raftNode_->leader_id(); }

butil::Status CopysetNode::TransferLeader(const Peer& peer) {
    butil::Status status;
    PeerId peerId(peer.address());

    if (raftNode_->leader_id() == peerId) {
        butil::Status status = butil::Status::OK();
        DVLOG(6) << "Skipped transferring leader to leader itself. "
                 << "peerid: " << peerId << ", Copyset: " << GroupIdString();

        return status;
    }

    int rc = raftNode_->transfer_leadership_to(peerId);
    if (rc != 0) {
        status = butil::Status(rc,
                               "Failed to transfer leader of copyset "
                               "%s to peer %s, error: %s",
                               GroupIdString().c_str(),
                               peerId.to_string().c_str(), berror(rc));
        LOG(ERROR) << status.error_str();

        return status;
    }
    transferee_ = peer;

    status = butil::Status::OK();
    LOG(INFO) << "Transferred leader of copyset " << GroupIdString()
              << " to peer " << peerId;

    return status;
}

butil::Status CopysetNode::AddPeer(const Peer& peer) {
    std::vector<PeerId> peers;
    PeerId peerId(peer.address());

    {
        std::unique_lock<std::mutex> lock_guard(confLock_);
        conf_.list_peers(&peers);
    }

    for (auto peer : peers) {
        if (peer == peerId) {
            butil::Status status = butil::Status::OK();
            DVLOG(6) << peerId << " is already a member of copyset "
                     << GroupIdString() << ", skip adding peer";

            return status;
        }
    }
    ConfigurationChangeDone* addPeerDone =
        new ConfigurationChangeDone(configChange_);
    ConfigurationChange expectedCfgChange(ConfigChangeType::ADD_PEER, peer);
    addPeerDone->expectedCfgChange = expectedCfgChange;
    raftNode_->add_peer(peerId, addPeerDone);
    if (addPeerDone->status().ok()) {
        *configChange_ = expectedCfgChange;
    }
    return addPeerDone->status();
}

butil::Status CopysetNode::RemovePeer(const Peer& peer) {
    std::vector<PeerId> peers;
    PeerId peerId(peer.address());

    {
        std::unique_lock<std::mutex> lock_guard(confLock_);
        conf_.list_peers(&peers);
    }

    bool peerValid = false;
    for (auto peer : peers) {
        if (peer == peerId) {
            peerValid = true;
            break;
        }
    }

    if (!peerValid) {
        butil::Status status = butil::Status::OK();
        DVLOG(6) << peerId << " is not a member of copyset " << GroupIdString()
                 << ", skip removing";

        return status;
    }
    ConfigurationChangeDone* removePeerDone =
        new ConfigurationChangeDone(configChange_);
    ConfigurationChange expectedCfgChange(ConfigChangeType::REMOVE_PEER, peer);
    removePeerDone->expectedCfgChange = expectedCfgChange;
    raftNode_->remove_peer(peerId, removePeerDone);
    if (removePeerDone->status().ok()) {
        *configChange_ = expectedCfgChange;
    }
    return removePeerDone->status();
}

butil::Status CopysetNode::ChangePeer(const std::vector<Peer>& newPeers) {
    std::vector<PeerId> newPeerIds;
    for (auto& peer : newPeers) {
        newPeerIds.emplace_back(peer.address());
    }
    Configuration newConf(newPeerIds);
    Configuration adding, removing;
    {
        std::unique_lock<std::mutex> lock_guard(confLock_);
        newConf.diffs(conf_, &adding, &removing);
    }
    butil::Status st;
    if (adding.size() != 1 || removing.size() != 1) {
        LOG(ERROR) << "Only change one peer to another is supported";
        st.set_error(EPERM, "only support change on peer to another");
        return st;
    }
    ConfigurationChangeDone* changePeerDone =
        new ConfigurationChangeDone(configChange_);
    ConfigurationChange expectedCfgChange;
    expectedCfgChange.type = ConfigChangeType::CHANGE_PEER;
    expectedCfgChange.alterPeer.set_address(adding.begin()->to_string());
    changePeerDone->expectedCfgChange = expectedCfgChange;
    raftNode_->change_peers(newConf, changePeerDone);
    if (changePeerDone->status().ok()) {
        *configChange_ = expectedCfgChange;
    }
    return changePeerDone->status();
}

void CopysetNode::UpdateAppliedIndex(uint64_t index) {
    uint64_t curIndex = appliedIndex_.load(std::memory_order_acquire);
    // Only update indexes larger than oneself
    if (index > curIndex) {
        /**
         * Explanation of compare_exchange_strong:
         * First, it compares whether curIndex is equal to appliedIndex. If it
         * is equal, it means that no one has modified appliedindex. In this
         * case, it tries to update appliedIndex with the value of index, and if
         * the update is successful, it's done. If curIndex is not equal to
         * appliedindex, it indicates that someone else has updated appliedIndex
         * in the meantime. In this case, it returns the current value of
         * appliedindex through curIndex and returns false. This entire process
         * is atomic.
         */
        while (!appliedIndex_.compare_exchange_strong(
            curIndex, index,
            std::memory_order_acq_rel)) {  // NOLINT
            if (index <= curIndex) {
                break;
            }
        }
    }
}

uint64_t CopysetNode::GetAppliedIndex() const {
    return appliedIndex_.load(std::memory_order_acquire);
}

std::shared_ptr<CSDataStore> CopysetNode::GetDataStore() const {
    return dataStore_;
}

CurveSegmentLogStorage* CopysetNode::GetLogStorage() const {
    return logStorage_;
}

ConcurrentApplyModule* CopysetNode::GetConcurrentApplyModule() const {
    return concurrentapply_;
}

void CopysetNode::Propose(const braft::Task& task) { raftNode_->apply(task); }

int CopysetNode::GetConfChange(ConfigChangeType* type, Configuration* oldConf,
                               Peer* alterPeer) {
    /**
     * To prevent inconsistencies between the epoch and configuration before
     * a new leader is elected and a noop entry is committed, consider the
     * following scenario:
     *
     * In a replication group with three members {ABC}, the current epoch is 5,
     * and A is the leader. A receives a configuration change log that adds D,
     * and assume that B also receives the configuration change log {ABC+D}.
     * Then, leader A crashes, and B is elected as the new leader. Before B
     * commits the noop entry, the maximum epoch value it can query on B is
     * still 5, but the queried configuration is {ABCD}. Therefore, here, before
     * the new leader B commits the noop entry, which is effectively committing
     * the hidden configuration change log {ABC+D}, it does not allow returning
     * the configuration and configuration change information to the user to
     * avoid epoch and configuration information inconsistency.
     */
    if (leaderTerm_.load(std::memory_order_acquire) <= 0) {
        *type = ConfigChangeType::NONE;
        return 0;
    }

    {
        std::lock_guard<std::mutex> lockguard(confLock_);
        *oldConf = conf_;
    }
    braft::NodeStatus status;
    raftNode_->get_status(&status);
    if (status.state == braft::State::STATE_TRANSFERRING) {
        *type = ConfigChangeType::TRANSFER_LEADER;
        *alterPeer = transferee_;
        return 0;
    }
    *type = configChange_->type;
    *alterPeer = configChange_->alterPeer;
    return 0;
}
uint64_t CopysetNode::LeaderTerm() const {
    return leaderTerm_.load(std::memory_order_acquire);
}

int CopysetNode::GetHash(std::string* hash) {
    int ret = 0;
    int fd = 0;
    int len = 0;
    uint32_t crc32c = 0;
    std::vector<std::string> files;

    ret = fs_->List(chunkDataApath_, &files);
    if (0 != ret) {
        return -1;
    }

    // Calculating all chunk files' crc requires ensuring that the order of
    // calculations is the same
    std::sort(files.begin(), files.end());

    for (std::string file : files) {
        std::string filename = chunkDataApath_;
        filename += "/";
        filename += file;
        fd = fs_->Open(filename.c_str(), O_RDONLY);
        if (0 >= fd) {
            return -1;
        }

        struct stat fileInfo;
        ret = fs_->Fstat(fd, &fileInfo);
        if (0 != ret) {
            return -1;
        }

        len = fileInfo.st_size;
        char* buff = new (std::nothrow) char[len];
        if (nullptr == buff) {
            return -1;
        }

        ret = fs_->Read(fd, buff, 0, len);
        if (ret != len) {
            delete[] buff;
            return -1;
        }

        crc32c = curve::common::CRC32(crc32c, buff, len);

        delete[] buff;
    }

    *hash = std::to_string(crc32c);

    return 0;
}

void CopysetNode::GetStatus(NodeStatus* status) {
    raftNode_->get_status(status);
}

void CopysetNode::GetLeaderLeaseStatus(braft::LeaderLeaseStatus* status) {
    raftNode_->get_leader_lease_status(status);
}

bool CopysetNode::GetLeaderStatus(NodeStatus* leaderStaus) {
    NodeStatus status;
    GetStatus(&status);
    if (status.leader_id.is_empty()) {
        return false;
    }
    if (status.leader_id == status.peer_id) {
        *leaderStaus = status;
        return true;
    }

    // get committed index from remote leader
    brpc::Controller cntl;
    cntl.set_timeout_ms(500);
    brpc::Channel channel;
    if (channel.Init(status.leader_id.addr, nullptr) != 0) {
        LOG(WARNING) << "can not create channel to " << status.leader_id.addr
                     << ", copyset " << GroupIdString();
        return false;
    }

    CopysetStatusRequest request;
    CopysetStatusResponse response;
    curve::common::Peer* peer = new curve::common::Peer();
    peer->set_address(status.leader_id.to_string());
    request.set_logicpoolid(logicPoolId_);
    request.set_copysetid(copysetId_);
    request.set_allocated_peer(peer);
    request.set_queryhash(false);

    CopysetService_Stub stub(&channel);
    stub.GetCopysetStatus(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG(WARNING) << "get leader status failed: " << cntl.ErrorText()
                     << ", copyset " << GroupIdString();
        return false;
    }

    if (response.status() != COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS) {
        LOG(WARNING) << "get leader status failed"
                     << ", status: " << response.status() << ", copyset "
                     << GroupIdString();
        return false;
    }

    leaderStaus->state = (braft::State)response.state();
    leaderStaus->peer_id.parse(response.peer().address());
    leaderStaus->leader_id.parse(response.leader().address());
    leaderStaus->readonly = response.readonly();
    leaderStaus->term = response.term();
    leaderStaus->committed_index = response.committedindex();
    leaderStaus->known_applied_index = response.knownappliedindex();
    leaderStaus->first_index = response.firstindex();
    leaderStaus->pending_index = response.pendingindex();
    leaderStaus->pending_queue_size = response.pendingqueuesize();
    leaderStaus->applying_index = response.applyingindex();
    leaderStaus->first_index = response.firstindex();
    leaderStaus->last_index = response.lastindex();
    leaderStaus->disk_index = response.diskindex();
    return true;
}

void CopysetNode::HandleSyncTimerOut() {
    if (isSyncing_.exchange(true)) {
        return;
    }
    SyncAllChunks();
    isSyncing_ = false;
}

void CopysetNode::ForceSyncAllChunks() {
    while (isSyncing_.exchange(true)) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(checkSyncingIntervalMs_));
    }
    SyncAllChunks();
    isSyncing_ = false;
}

void CopysetNode::SyncAllChunks() {
    std::deque<ChunkID> temp;
    {
        curve::common::LockGuard lg(chunkIdsLock_);
        temp.swap(chunkIdsToSync_);
    }
    std::set<ChunkID> chunkIds;
    for (auto chunkId : temp) {
        chunkIds.insert(chunkId);
    }
    for (ChunkID chunk : chunkIds) {
        copysetSyncPool_->Enqueue([=]() {
            CSErrorCode r = dataStore_->SyncChunk(chunk);
            if (r != CSErrorCode::Success) {
                LOG(FATAL) << "Sync Chunk failed in Copyset: "
                           << GroupIdString() << ", chunkid: " << chunk
                           << " data store return: " << r;
            }
        });
    }
}

void SyncChunkThread::Init(CopysetNode* node) {
    running_ = true;
    node_ = node;
    cond_ = std::make_shared<std::condition_variable>();
}

void SyncChunkThread::Run() {
    syncThread_ = std::thread([this]() {
        while (running_) {
            std::unique_lock<std::mutex> lock(mtx_);
            cond_->wait_for(
                lock, std::chrono::seconds(CopysetNode::syncTriggerSeconds_));
            node_->SyncAllChunks();
        }
    });
}

void SyncChunkThread::Stop() {
    running_ = false;
    if (syncThread_.joinable()) {
        cond_->notify_one();
        syncThread_.join();
    }
}

SyncChunkThread::~SyncChunkThread() { Stop(); }

}  // namespace chunkserver
}  // namespace curve

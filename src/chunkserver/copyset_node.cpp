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

#include <glog/logging.h>
#include <brpc/controller.h>
#include <butil/sys_byteorder.h>
#include <braft/closure_helper.h>
#include <braft/snapshot.h>
#include <braft/protobuf_file.h>
#include <utility>
#include <memory>
#include <algorithm>
#include <cassert>

#include "src/chunkserver/raftsnapshot/curve_filesystem_adaptor.h"
#include "src/chunkserver/chunk_closure.h"
#include "src/chunkserver/op_request.h"
#include "src/fs/fs_common.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/datastore/datastore_file_helper.h"
#include "src/chunkserver/uri_paser.h"
#include "src/common/crc32.h"
#include "src/common/fs_util.h"

namespace curve {
namespace chunkserver {

using curve::fs::FileSystemInfo;

const char *kCurveConfEpochFilename = "conf.epoch";

CopysetNode::CopysetNode(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId,
                         const Configuration &initConf) :
    logicPoolId_(logicPoolId),
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
    configChange_(std::make_shared<ConfigurationChange>()) {
}

CopysetNode::~CopysetNode() {
    // Remove copyset's metric
    ChunkServerMetric::GetInstance()->RemoveCopysetMetric(logicPoolId_,
                                                          copysetId_);
    metric_ = nullptr;

    if (nodeOptions_.snapshot_file_system_adaptor != nullptr) {
        delete nodeOptions_.snapshot_file_system_adaptor;
        nodeOptions_.snapshot_file_system_adaptor = nullptr;
    }
    LOG(INFO) << "release copyset node: "
              << GroupIdString();
}

int CopysetNode::Init(const CopysetNodeOptions &options) {
    std::string groupId = GroupId();

    std::string protocol = UriParser::ParseUri(options.chunkDataUri,
                                                &copysetDirPath_);
    if (protocol.empty()) {
        // TODO(wudemiao): Add the necessary error codes and return
        LOG(ERROR) << "not support chunk data uri's protocol"
                   << " error chunkDataDir is: " << options.chunkDataUri
                   << ". Copyset: " << GroupIdString();
        return -1;
    }

    /**
     * Init copyset node's configuration about chunk server .
     * Both of these must be initialized before draftNode_.init
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
    dsOptions.pageSize = options.pageSize;
    dsOptions.locationLimit = options.locationLimit;
    dataStore_ = std::make_shared<CSDataStore>(options.localFileSystem,
                                               options.chunkFilePool,
                                               dsOptions);
    CHECK(nullptr != dataStore_);
    if (false == dataStore_->Initialize()) {
        // TODO(wudemiao): Add the necessary error codes and return
        LOG(ERROR) << "data store init failed. "
                   << "Copyset: " << GroupIdString();
        return -1;
    }

    recyclerUri_ = options.recyclerUri;

    // TODO(wudemiao): Put in the init of nodeOptions
    /**
     * Init copyset's corresponding raft node options
     */
    nodeOptions_.initial_conf = conf_;
    nodeOptions_.election_timeout_ms = options.electionTimeoutMs;
    nodeOptions_.fsm = this;
    nodeOptions_.node_owns_fsm = false;
    nodeOptions_.snapshot_interval_s = options.snapshotIntervalS;
    nodeOptions_.log_uri = options.logUri;
    nodeOptions_.log_uri.append("/").append(groupId)
        .append("/").append(RAFT_LOG_DIR);
    nodeOptions_.raft_meta_uri = options.raftMetaUri;
    nodeOptions_.raft_meta_uri.append("/").append(groupId)
        .append("/").append(RAFT_META_DIR);
    nodeOptions_.snapshot_uri = options.raftSnapshotUri;
    nodeOptions_.snapshot_uri.append("/").append(groupId)
        .append("/").append(RAFT_SNAP_DIR);
    nodeOptions_.usercode_in_pthread = options.usercodeInPthread;
    nodeOptions_.snapshot_throttle = options.snapshotThrottle;

    CurveFilesystemAdaptor* cfa =
        new CurveFilesystemAdaptor(options.chunkFilePool,
                                   options.localFileSystem);
    std::vector<std::string> filterList;
    std::string snapshotMeta(BRAFT_SNAPSHOT_META_FILE);
    filterList.push_back(kCurveConfEpochFilename);
    filterList.push_back(snapshotMeta);
    filterList.push_back(snapshotMeta.append(BRAFT_PROTOBUF_FILE_TEMP));
    cfa->SetFilterList(filterList);

    nodeOptions_.snapshot_file_system_adaptor =
        new scoped_refptr<braft::FileSystemAdaptor>(cfa);

    /* init peer id */
    butil::ip_t ip;
    butil::str2ip(options.ip.c_str(), &ip);
    butil::EndPoint addr(ip, options.port);
    /**
     * idx is default zero, and multiple copies of the same copyset are not
     * allowed for one process in chunkserver
     * this is different from braft
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
    metric_ = ChunkServerMetric::GetInstance()->GetCopysetMetric(
        logicPoolId_, copysetId_);
    if (metric_ != nullptr) {
        // TODO(yyk): Add a datastore-level io metric
        metric_->MonitorDataStore(dataStore_.get());
    }

    return 0;
}

int CopysetNode::Run() {
    // The initialization of the raft node actually makes the copyset run
    if (0 != raftNode_->init(nodeOptions_)) {
        LOG(ERROR) << "Fail to init raft node. "
                   << "Copyset: " << GroupIdString();
        return -1;
    }
    LOG(INFO) << "Run copyset success."
              << "Copyset: " << GroupIdString();
    return 0;
}

void CopysetNode::Fini() {
    if (nullptr != raftNode_) {
        // Shut down all services on this raft node
        raftNode_->shutdown(nullptr);
        // Wait for all the tasks being processed to finish
        raftNode_->join();
    }
    if (nullptr != concurrentapply_) {
        // Drop unflushed data to disk, if not flushed
        // When transferring a copyset, it may be wrong to perform a WriteChunk
        // operation after the copyset has been removed.
        concurrentapply_->Flush();
    }
}

void CopysetNode::on_apply(::braft::Iterator &iter) {
    for (; iter.valid(); iter.next()) {
        // Asynchronous execution in bthread is to avoid blocking the execution
        // of the current state machine
        braft::AsyncClosureGuard doneGuard(iter.done());

        /**
         * Get the ChunkClosure passed to braft when submitting the task,
         * which contains all the contexts of the Op ChunkOpRequest
         */
        braft::Closure *closure = iter.done();

        if (nullptr != closure) {
            /**
             * 1.If the closure is not null, then the current node is normal,
             * so we get the Op context directly from memory for apply
             */
            ChunkClosure
                *chunkClosure = dynamic_cast<ChunkClosure *>(iter.done());
            CHECK(nullptr != chunkClosure)
                << "ChunkClosure dynamic cast failed";
            std::shared_ptr<ChunkOpRequest> opRequest = chunkClosure->request_;
            auto task = std::bind(&ChunkOpRequest::OnApply,
                                  opRequest,
                                  iter.index(),
                                  doneGuard.release());
            concurrentapply_->Push(
                opRequest->ChunkId(), opRequest->OpType(), task);
        } else {
            // get log entry
            butil::IOBuf log = iter.data();
            /**
             * 2.When closure is null，there are two cases：
             * 2.1. The node restarts and plays back the apply, where the Op log
             * entry is deserialized and then the Op information is retrieved
             * for apply
             * 2.2. follower apply
             */
            ChunkRequest request;
            butil::IOBuf data;
            auto opReq = ChunkOpRequest::Decode(log, &request, &data);
            auto chunkId = request.chunkid();
            auto task = std::bind(&ChunkOpRequest::OnApplyFromLog,
                                  opReq,
                                  dataStore_,
                                  std::move(request),
                                  data);
            concurrentapply_->Push(chunkId, request.optype(), task);
        }
    }
}

void CopysetNode::on_shutdown() {
    LOG(INFO) << GroupIdString() << " is shutdown";
}

void CopysetNode::on_snapshot_save(::braft::SnapshotWriter *writer,
                                   ::braft::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    /**
     * 1.flush I/O to disk，to ensure that all data is on disk
     */
    concurrentapply_->Flush();

    /**
     * 2.Save the configuration epoch: conf.epoch, note that conf.epoch is
     * stored in the data directory
     */
    std::string
        filePathTemp = writer->get_path() + "/" + kCurveConfEpochFilename;
    if (0 != SaveConfEpoch(filePathTemp)) {
        done->status().set_error(errno, "invalid: %s", strerror(errno));
        LOG(ERROR) << "SaveConfEpoch failed. "
                   << "Copyset: " << GroupIdString()
                   << ", errno: " << errno << ", "
                   << ", error message: " << strerror(errno);
        return;
    }

    /**
     * 3.Save a list of chunk filenames to the snapshot metadata file
     */
    std::vector<std::string> files;
    if (0 == fs_->List(chunkDataApath_, &files)) {
        for (const auto& fileName : files) {
            // When raft saves a snapshot, it is not necessary to save a list
            // of snapshot files in the meta message
            // After downloading the chunk, raft will fetch the snapshot list
            // separately
            bool isSnapshot = DatastoreFileHelper::IsSnapshotFile(fileName);
            if (isSnapshot) {
                continue;
            }
            std::string chunkApath;
            // Calculate the path relative to the snapshot directory from the
            // absolute path
            chunkApath.append(chunkDataApath_);
            chunkApath.append("/").append(fileName);
            std::string filePath = curve::common::CalcRelativePath(
                                    writer->get_path(), chunkApath);
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

int CopysetNode::on_snapshot_load(::braft::SnapshotReader *reader) {
    /**
     * 1. Load snapshot data
     */
    // Opened snapshot path: /mnt/sda/1-10001/raft_snapshot/snapshot_0043
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
        // Clean up the files in the copyset data directory before loading the
        // snapshot data,otherwise there may be some remaining data after the
        // snapshot is loaded. If delete_file fails or rename fails, the current
        // node status will be set to ERROR. If the process is restarted during
        // delete_file or rename, the snapshot will be loaded when copyset is
        // available. Since rename guarantees atomicity, the data directory
        // will definitely be restored after the snapshot is loaded
        bool ret = nodeOptions_.snapshot_file_system_adaptor->get()->
                                delete_file(chunkDataApath_, true);
        if (!ret) {
            LOG(ERROR) << "delete chunk data dir failed. "
                       << "Copyset: " << GroupIdString()
                       << ", path: " << chunkDataApath_;
            return -1;
        }
        LOG(INFO) << "delete chunk data dir success. "
                  << "Copyset: " << GroupIdString()
                  << ", path: " << chunkDataApath_;
        ret = nodeOptions_.snapshot_file_system_adaptor->get()->
                           rename(snapshotChunkDataDir, chunkDataApath_);
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
        LOG(INFO) << "load snapshot data path: "
                  << snapshotChunkDataDir << " not exist. "
                  << "Copyset: " << GroupIdString();
    }

    /**
     * 2. Load configuration epoch files
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
     * 3.Reinit data store, example of scenario:
     *
     * (1) For example, if we first add a peer and then read it, the data
     * store  will return that the chunk does not exist, because the new peer
     * did not have any data when it was first added. At this point the data
     * store initiates, then the added peer is not sensed by the data store
     * after the leader has recovered the data.
     *
     * (2) The peer restores all the data by installing snapshot, which is
     * done by rename. If a file was previously opened by the data store,
     * then rename will work. But the old file can only be deleted when the
     * data store closes the old file, so you need to re-init data store and
     * close the fd of the file, and then re-open the new file, otherwise the
     * data store will always be operating on the old file.
     * Once the data store has closed the corresponding fd once, the data
     * written later will be lost. In addition, if the datastore init does
     * not re-open the file, it will not read the recovered data, but the old
     * data.
     */
    if (!dataStore_->Initialize()) {
        LOG(ERROR) << "data store init failed in on snapshot load. "
                   << "Copyset: " << GroupIdString();
        return -1;
    }

    /**
     * 4.If conf is stored in snapshot, then load the initialization,
     * ensuring that no on_configuration_committed is needed. Note that the
     * joint stage log is ignored here.
     */
    braft::SnapshotMeta meta;
    reader->load_meta(&meta);
    if (0 == meta.old_peers_size()) {
        conf_.reset();
        for (int i = 0; i < meta.peers_size(); ++i) {
            conf_.add_peer(meta.peers(i));
        }
    }
    lastSnapshotIndex_ = meta.last_included_index();
    return 0;
}

void CopysetNode::on_leader_start(int64_t term) {
    leaderTerm_.store(term, std::memory_order_release);
    ChunkServerMetric::GetInstance()->IncreaseLeaderCount();
    concurrentapply_->Flush();
    LOG(INFO) << "Copyset: " << GroupIdString()
              << ", peer id: " << peerId_.to_string()
              << " become leader, term is: " << leaderTerm_;
}

void CopysetNode::on_leader_stop(const butil::Status &status) {
    leaderTerm_.store(-1, std::memory_order_release);
    ChunkServerMetric::GetInstance()->DecreaseLeaderCount();
    LOG(INFO) << "Copyset: " << GroupIdString()
              << ", peer id: " << peerId_.to_string() << " stepped down";
}

void CopysetNode::on_error(const ::braft::Error &e) {
    LOG(FATAL) << "Copyset: " << GroupIdString()
               << ", peer id: " << peerId_.to_string()
               << " meet raft error: " << e;
}

void CopysetNode::on_configuration_committed(const Configuration& conf,
                                             int64_t index) {
    // This function is also called when loading snapshot.
    // Loading snapshot should not increase epoch. When loading
    // snapshot, the index is equal with lastSnapshotIndex_.
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

void CopysetNode::on_stop_following(const ::braft::LeaderChangeContext &ctx) {
    LOG(INFO) << "Copyset: " << GroupIdString()
              << ", peer id: " << peerId_.to_string()
              << " stops following" << ctx;
}

void CopysetNode::on_start_following(const ::braft::LeaderChangeContext &ctx) {
    LOG(INFO) << "Copyset: " << GroupIdString()
              << ", peer id: " << peerId_.to_string()
              << "start following" << ctx;
}

LogicPoolID CopysetNode::GetLogicPoolId() const {
    return logicPoolId_;
}

CopysetID CopysetNode::GetCopysetId() const {
    return copysetId_;
}

std::string CopysetNode::GetCopysetDir() const {
    return copysetDirPath_;
}

uint64_t CopysetNode::GetConfEpoch() const {
    std::lock_guard<std::mutex> lockguard(confLock_);
    return epoch_.load(std::memory_order_relaxed);
}

int CopysetNode::LoadConfEpoch(const std::string &filePath) {
    LogicPoolID loadLogicPoolID = 0;
    CopysetID loadCopysetID = 0;
    uint64_t loadEpoch = 0;

    int ret = epochFile_->Load(filePath,
                               &loadLogicPoolID,
                               &loadCopysetID,
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

int CopysetNode::SaveConfEpoch(const std::string &filePath) {
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

void CopysetNode::SetSnapshotFileSystem(scoped_refptr<FileSystemAdaptor> *fs) {
    nodeOptions_.snapshot_file_system_adaptor = fs;
}

bool CopysetNode::IsLeaderTerm() const {
    if (0 < leaderTerm_.load(std::memory_order_acquire))
        return true;
    return false;
}

PeerId CopysetNode::GetLeaderId() const {
    return raftNode_->leader_id();
}

butil::Status CopysetNode::TransferLeader(const Peer& peer) {
    butil::Status status;
    PeerId peerId(peer.address());

    if (raftNode_->leader_id() == peerId) {
        butil::Status status = butil::Status::OK();
        DVLOG(6) << "Skipped transferring leader to leader itself. "
                 << "peerid: " << peerId
                 << ", Copyset: " << GroupIdString();

        return status;
    }

    int rc = raftNode_->transfer_leadership_to(peerId);
    if (rc != 0) {
        status = butil::Status(rc, "Failed to transfer leader of copyset "
                               "%s to peer %s, error: %s",
                               GroupIdString().c_str(),
                               peerId.to_string().c_str(), berror(rc));
        LOG(ERROR) << status.error_str();

        return status;
    }
    transferee_ = peer;

    status = butil::Status::OK();
    LOG(INFO) << "Transferred leader of copyset "
              << GroupIdString()
              << " to peer " <<  peerId;

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
                     << GroupIdString()
                     << ", skip adding peer";

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
        DVLOG(6) << peerId << " is not a member of copyset "
                 << GroupIdString() << ", skip removing";

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
    // Only update indexes that are larger than itself
    if (index > curIndex) {
        /**
         * compare_exchange_strong explanation：
         * First we compare whether the curIndex is equal to the
         * appliedIndex,  if it is, then no one has modified the appliedindex.
         * Then the index is used to modify the appliedIndex and the update
         * succeeds.
         * If it is not equal, then someone has updated the appliedindex, so the
         * current appliedindex is returned by curIndex and false is returned.
         * The whole process is atomic。
         */
        while (!appliedIndex_.compare_exchange_strong(curIndex,
                                                      index,
                                                      std::memory_order_acq_rel)) { //NOLINT
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

ConcurrentApplyModule *CopysetNode::GetConcurrentApplyModule() const {
    return concurrentapply_;
}

void CopysetNode::Propose(const braft::Task &task) {
    raftNode_->apply(task);
}

int CopysetNode::GetConfChange(ConfigChangeType *type,
                               Configuration *oldConf,
                               Peer *alterPeer) {
    /**
     * To avoid a situation where the epoch and configuration of the new
     * leader may not match before the noop entry is submitted and after the
     * leader has been elected, consider the following scenario:
     * A three-member copyset {ABC}, current epoch=5, A is the leader and
     * receives configuration +D. Suppose B receives a configuration change
     * log for {ABC+D}, then leader A fails, and B is elected as new leader.
     * Before B commits a noop entry, the maximum possible epoch value on B is
     * 5, and the configuration queried is indeed {ABCD}. So here, before new
     * leader B submits the noop entry, that is, before the configuration change
     * log {ABC+D} is submitted, the configuration and configuration change
     * information is not allowed to be returned to the user to avoid
     * inconsistency between epoch and configuration information.
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

int CopysetNode::GetHash(std::string *hash) {
    int ret = 0;
    int fd  = 0;
    int len = 0;
    uint32_t crc32c = 0;
    std::vector<std::string> files;

    ret = fs_->List(chunkDataApath_, &files);
    if (0 != ret) {
        return -1;
    }

    // To calculate the crc of all chunk files, you need to ensure that the
    // order of calculation is the same.
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
        char *buff = new (std::nothrow) char[len];
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

void CopysetNode::GetStatus(NodeStatus *status) {
    raftNode_->get_status(status);
}

bool CopysetNode::GetLeaderStatus(NodeStatus *leaderStaus) {
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
    if (channel.Init(status.leader_id.addr, nullptr) !=0) {
        LOG(WARNING) << "can not create channel to "
                     << status.leader_id.addr
                     << ", copyset " << GroupIdString();
        return false;
    }

    CopysetStatusRequest request;
    CopysetStatusResponse response;
    curve::common::Peer *peer = new curve::common::Peer();
    peer->set_address(status.leader_id.to_string());
    request.set_logicpoolid(logicPoolId_);
    request.set_copysetid(copysetId_);
    request.set_allocated_peer(peer);
    request.set_queryhash(false);

    CopysetService_Stub stub(&channel);
    stub.GetCopysetStatus(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG(WARNING) << "get leader status failed: "
                     << cntl.ErrorText()
                     << ", copyset " << GroupIdString();
        return false;
    }

    if (response.status() != COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS) {
        LOG(WARNING) << "get leader status failed"
                     << ", status: " << response.status()
                     << ", copyset " << GroupIdString();
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

}  // namespace chunkserver
}  // namespace curve

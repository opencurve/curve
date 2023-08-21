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
#include <future>
#include <deque>
#include <set>
#include <chrono>
#include <condition_variable>

#include "src/chunkserver/raftsnapshot/curve_filesystem_adaptor.h"
#include "src/chunkserver/chunk_closure.h"
#include "src/chunkserver/op_request.h"
#include "src/common/concurrent/task_thread_pool.h"
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

uint32_t CopysetNode::syncTriggerSeconds_ = 25;
std::shared_ptr<common::TaskThreadPool<>>
    CopysetNode::copysetSyncPool_ = nullptr;

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
    lastSnapshotIndex_(0),
    configChange_(std::make_shared<ConfigurationChange>()),
    enableOdsyncWhenOpenChunkFile_(false),
    isSyncing_(false),
    checkSyncingIntervalMs_(500) {
}

CopysetNode::~CopysetNode() {
    // 移除 copyset的metric
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
        // TODO(wudemiao): 增加必要的错误码并返回
        LOG(ERROR) << "not support chunk data uri's protocol"
                   << " error chunkDataDir is: " << options.chunkDataUri
                   << ". Copyset: " << GroupIdString();
        return -1;
    }

    /**
     * Init copyset node关于chunk server的配置，
     * 这两个的初始化必须在raftNode_.init之前
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
    dataStore_ = std::make_shared<CSDataStore>(options.localFileSystem,
                                               options.chunkFilePool,
                                               dsOptions);
    CHECK(nullptr != dataStore_);
    if (false == dataStore_->Initialize()) {
        // TODO(wudemiao): 增加必要的错误码并返回
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
                  << options.syncChunkLimit <<
                  "syncthreshold = " << options.syncThreshold;
    }

    recyclerUri_ = options.recyclerUri;

    // TODO(wudemiao): 放到nodeOptions的init中
    /**
     * Init copyset对应的raft node options
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

    /* 初始化 peer id */
    butil::ip_t ip;
    butil::str2ip(options.ip.c_str(), &ip);
    butil::EndPoint addr(ip, options.port);
    /**
     * idx默认是零，在chunkserver不允许一个进程有同一个copyset的多副本，
     * 这一点注意和不让braft区别开来
     */
    peerId_ = PeerId(addr, 0);
    raftNode_ = std::make_shared<RaftNode>(groupId, peerId_);
    concurrentapply_ = options.concurrentapply;

    /*
     * 初始化copyset性能metrics
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
        // TODO(yyk) 后续考虑添加datastore层面的io metric
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
    // raft node的初始化实际上让起run起来
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
        // 关闭所有关于此raft node的服务
        raftNode_->shutdown(nullptr);
        // 等待所有的正在处理的task结束
        raftNode_->join();
    }
    if (nullptr != concurrentapply_) {
        // 将未刷盘的数据落盘，如果不刷盘
        // 迁移copyset时，copyset移除后再去执行WriteChunk操作可能出错
        concurrentapply_->Flush();
    }
}

//compute hashcode by fileid and chunkindex of request
uint64_t CopysetNode::GetHashCode(const ChunkRequest* request) {
    uint64_t hashcode = 0;
    uint64_t filedid = 0;
    uint64_t chunkindex = 0;
    
    //judge if has originfileId
    if (request->has_originfileid()) {//for clone file, it has originfileId, all file with same originfileid need to serialize
        switch (request->optype()) {
        case CHUNK_OP_DELETE:
        case CHUNK_OP_WRITE:
        case CHUNK_OP_DELETE_SNAP:
        case CHUNK_OP_FLATTEN:
            filedid = request->originfileid();
            chunkindex = request->chunkindex();
            hashcode = filedid ^ (chunkindex << 1);
            break;
        
        default:
            hashcode = std::hash<uint64_t>{} (request->fileid());
            break;
        }
    } else { //has no originfileId it is the origin file just use the fileid
        switch (request->optype()) {
        case CHUNK_OP_DELETE:
        case CHUNK_OP_WRITE:
        case CHUNK_OP_DELETE_SNAP:
        case CHUNK_OP_FLATTEN:
            filedid = request->fileid();
            chunkindex = request->chunkindex();
            hashcode = filedid ^ (chunkindex << 1);
            break;

        default:
            hashcode = std::hash<uint64_t>{} (request->fileid());
            break;
        }
    }

    return hashcode;
}

void CopysetNode::on_apply(::braft::Iterator &iter) {
    for (; iter.valid(); iter.next()) {
        // 放在bthread中异步执行，避免阻塞当前状态机的执行
        braft::AsyncClosureGuard doneGuard(iter.done());

        /**
         * 获取向braft提交任务时候传递的ChunkClosure，里面包含了
         * Op的所有上下文 ChunkOpRequest
         */
        braft::Closure *closure = iter.done();

        if (nullptr != closure) {
            /**
             * 1.closure不是null，那么说明当前节点正常，直接从内存中拿到Op
             * context进行apply
             */
            ChunkClosure
                *chunkClosure = dynamic_cast<ChunkClosure *>(iter.done());
            CHECK(nullptr != chunkClosure)
                << "ChunkClosure dynamic cast failed";
            std::shared_ptr<ChunkOpRequest> opRequest = chunkClosure->request_;

            uint64_t hashcode = GetHashCode(opRequest->GetChunkRequest());
            auto task = std::bind(&ChunkOpRequest::OnApply,
                                  opRequest,
                                  iter.index(),
                                  doneGuard.release());
            concurrentapply_->Push(
                hashcode, opRequest->OpType(), task);
        } else {
            // 获取log entry
            butil::IOBuf log = iter.data();
            /**
             * 2.closure是null，有两种情况：
             * 2.1. 节点重启，回放apply，这里会将Op log entry进行反序列化，
             * 然后获取Op信息进行apply
             * 2.2. follower apply
             */
            ChunkRequest request;
            butil::IOBuf data;
            auto opReq = ChunkOpRequest::Decode(log, &request, &data, shared_from_this());
            auto chunkId = request.chunkid();
            uint64_t hashcode = GetHashCode(&request);
            auto task = std::bind(&ChunkOpRequest::OnApplyFromLog,
                                  opReq,
                                  dataStore_,
                                  std::move(request),
                                  data);
            concurrentapply_->Push(hashcode, request.optype(), task);
        }
    }
}

void CopysetNode::on_shutdown() {
    LOG(INFO) << GroupIdString() << " is shutdown";
}

void CopysetNode::on_snapshot_save(::braft::SnapshotWriter *writer,
                                   ::braft::Closure *done) {
    snapshotFuture_ =
        std::async(std::launch::async,
            &CopysetNode::save_snapshot_background, this, writer, done);
}

void CopysetNode::WaitSnapshotDone() {
    // wait async snapshot done
    if (snapshotFuture_.valid()) {
        snapshotFuture_.wait();
    }
}

void CopysetNode::save_snapshot_background(::braft::SnapshotWriter *writer,
                                   ::braft::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    /**
     * 1.flush I/O to disk，确保数据都落盘
     */
    concurrentapply_->Flush();

    if (!enableOdsyncWhenOpenChunkFile_) {
        ForceSyncAllChunks();
    }

    /**
     * 2.保存配置版本: conf.epoch，注意conf.epoch是存放在data目录下
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
     * 3.保存chunk文件名的列表到快照元数据文件中
     */
    std::vector<std::string> files;
    if (0 == fs_->List(chunkDataApath_, &files)) {
        for (const auto& fileName : files) {
            // raft保存快照时，meta信息中不用保存快照文件列表
            // raft下载快照的时候，在下载完chunk以后，会单独获取snapshot列表
            bool isSnapshot = DatastoreFileHelper::IsSnapshotFile(fileName);
            if (isSnapshot) {
                continue;
            }

            std::string chunkApath;
            // 通过绝对路径，算出相对于快照目录的路径
            chunkApath.append(chunkDataApath_);
            chunkApath.append("/").append(fileName);
            std::string filePath = curve::common::CalcRelativePath(
                                    writer->get_path(), chunkApath);
            
            writer->add_file(filePath);
        }

        //add the chunkid --> cloneno map into the filepath
        //using the ._ as the prefix

        std::vector<CloneListInfo> cloneList;
        dataStore_->GetCloneInfoList(cloneList);

        for (auto& cloneInfo : cloneList) {
            std::string chunkApath;
            chunkApath.append(chunkDataApath_);
            std::string fileName = "._" + std::to_string(cloneInfo.chunkid) + "_" + std::to_string(cloneInfo.cloneNo);
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
     * 4. 保存conf.epoch文件到快照元数据文件中
     */
     writer->add_file(kCurveConfEpochFilename);
}

int CopysetNode::on_snapshot_load(::braft::SnapshotReader *reader) {
    /**
     * 1. 加载快照数据
     */
    // 打开的 snapshot path: /mnt/sda/1-10001/raft_snapshot/snapshot_0043
    std::string snapshotPath = reader->get_path();

    // /mnt/sda/1-10001/raft_snapshot/snapshot_0043/data
    std::string snapshotChunkDataDir;
    snapshotChunkDataDir.append(snapshotPath);
    snapshotChunkDataDir.append("/").append(chunkDataRpath_);
    LOG(INFO) << "load snapshot data path: " << snapshotChunkDataDir
              << ", Copyset: " << GroupIdString();
    // 如果数据目录不存在，那么说明 load snapshot 数据部分就不需要处理
    if (fs_->DirExists(snapshotChunkDataDir)) {
        // 加载快照数据前，要先清理copyset data目录下的文件
        // 否则可能导致快照加载以后存在一些残留的数据
        // 如果delete_file失败或者rename失败，当前node状态会置为ERROR
        // 如果delete_file或者rename期间进程重启，copyset起来后会加载快照
        // 由于rename可以保证原子性，所以起来加载快照后，data目录一定能还原
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
     * 2. 加载配置版本文件
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
     * 3.重新init data store，场景举例：
     *
     * (1) 例如一个add peer，之后立马read这个时候data store会返回chunk
     * not exist，因为这个新增的peer在刚开始起来的时候，没有任何数据，这
     * 个时候data store init了，那么新增的peer在leader恢复了数据之后，
     * data store并不感知；
     *
     * (2) peer通过install snapshot恢复了所有的数据是通过rename操作的，
     * 如果某个file之前被data store打开了，那么rename能成功，但是老的
     * 文件只有等data store close老的文件才能删除，所以需要重新init data
     * store，并且close的文件的fd，然后重新open新的文件，不然data store
     * 会一直是操作的老的文件，而一旦data store close相应的fd一次之后，
     * 后面的write的数据就会丢，除此之外，如果 datastore init没有重新open
     * 文件，也将导致read不到恢复过来的数据，而是read到老的数据。
     */
    if (!dataStore_->Initialize()) {
        LOG(ERROR) << "data store init failed in on snapshot load. "
                   << "Copyset: " << GroupIdString();
        return -1;
    }

    /**
     * 4.如果snapshot中存 conf，那么加载初始化，保证不需要以来
     * on_configuration_committed。需要注意的是这里会忽略joint stage的日志。
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
    ChunkServerMetric::GetInstance()->IncreaseLeaderCount();
    concurrentapply_->Flush();
    leaderTerm_.store(term, std::memory_order_release);
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
    // 只更新比自己大的 index
    if (index > curIndex) {
        /**
         * compare_exchange_strong解释：
         * 首先比较curIndex是不是等于appliedIndex，如果是，那么说明没有人
         * 修改appliedindex，那么用index去修改appliedIndex，更新成功，完成；
         * 如果不等于，说明有人更新了appliedindex，那么通过curIndex返回当前
         * 的appliedindex，并且返回false。整个过程都是原子的
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

CurveSegmentLogStorage* CopysetNode::GetLogStorage() const {
    return logStorage_;
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
     * 避免new leader当选leader之后，提交noop entry之前，epoch和
     * 配置可能不一致的情况。考虑如下情形：
     *
     * 三个成员的复制组{ABC}，当前epoch=5，A是leader，收到配置配置+D，
     * 假设B收到了{ABC+D}的配置变更日志，然后leader A挂了，B当选为了
     * new leader，在B提交noop entry之前，B上查询到的epoch值最大可能为5，
     * 而查询到的配置确实{ABCD}了，所以这里在new leader B在提交noop entry
     * 之前，也就是实现隐公提交配置变更日志{ABC+D}之前，不允许向用户返回
     * 配置和配置变更信息，避免epoch和配置信息不一致
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

    // 计算所有chunk文件crc需要保证计算的顺序是一样的
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
                       << GroupIdString()
                       << ", chunkid: " << chunk
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
    syncThread_ = std::thread([this](){
        while (running_) {
            std::unique_lock<std::mutex> lock(mtx_);
            cond_->wait_for(lock,
                std::chrono::seconds(CopysetNode::syncTriggerSeconds_));
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

SyncChunkThread::~SyncChunkThread() {
    Stop();
}

}  // namespace chunkserver
}  // namespace curve

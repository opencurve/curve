/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
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

#include "src/chunkserver/raftsnapshot_filesystem_adaptor.h"
#include "src/chunkserver/chunk_closure.h"
#include "src/chunkserver/op_request.h"
#include "src/fs/fs_common.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"
#include "src/common/crc32.h"

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
    leaderTerm_(-1) {
}

CopysetNode::~CopysetNode() {
    // 移除 copyset的metric
    ChunkServerMetric::GetInstance()->RemoveCopysetMetric(logicPoolId_,
                                                          copysetId_);
    metric_ = nullptr;

    if (nodeOptions_.snapshot_file_system_adaptor != nullptr) {
        delete nodeOptions_.snapshot_file_system_adaptor;
        nodeOptions_.snapshot_file_system_adaptor = nullptr;
        LOG(INFO) << "release raftsnapshot filesystem adaptor!";
    }
}

int CopysetNode::Init(const CopysetNodeOptions &options) {
    std::string groupId = ToGroupId(logicPoolId_, copysetId_);

    std::string protocol = FsAdaptorUtil::ParserUri(options.chunkDataUri,
                                                    &copysetDirPath_);
    if (protocol.empty()) {
        // TODO(wudemiao): 增加必要的错误码并返回
        LOG(ERROR) << "not support chunk data uri's protocol"
                   << " error chunkDataDir is: " << options.chunkDataUri;
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
    epochFile_ = std::make_unique<ConfEpochFile>(fs_);

    chunkDataRpath_ = RAFT_DATA_DIR;
    chunkDataApath_.append("/").append(RAFT_DATA_DIR);
    DataStoreOptions dsOptions;
    dsOptions.baseDir = chunkDataApath_;
    dsOptions.chunkSize = options.maxChunkSize;
    dsOptions.pageSize = options.pageSize;
    dataStore_ = std::make_shared<CSDataStore>(options.localFileSystem,
                                               options.chunkfilePool,
                                               dsOptions);
    CHECK(nullptr != dataStore_);
    if (false == dataStore_->Initialize()) {
        // TODO(wudemiao): 增加必要的错误码并返回
        LOG(ERROR) << "data store init failed, "
                   << errno << " : " << strerror(errno);
        return -1;
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

    RaftSnapshotFilesystemAdaptor* rfa =
        new RaftSnapshotFilesystemAdaptor(options.chunkfilePool,
                                          options.localFileSystem);
    std::vector<std::string> filterList;
    std::string snapshotMeta(BRAFT_SNAPSHOT_META_FILE);
    filterList.push_back(kCurveConfEpochFilename);
    filterList.push_back(snapshotMeta);
    filterList.push_back(snapshotMeta.append(BRAFT_PROTOBUF_FILE_TEMP));
    rfa->SetFilterList(filterList);

    nodeOptions_.snapshot_file_system_adaptor =
        new scoped_refptr<braft::FileSystemAdaptor>(rfa);

    /* 初始化 peer id */
    butil::ip_t ip;
    butil::str2ip(options.ip.c_str(), &ip);
    butil::EndPoint addr(ip, options.port);
    /**
     * idx默认是零，在chunkserver不允许一个进程有同一个copyset的多副本，
     * 这一点注意和不让braft区别开来
     */
    peerId_ = PeerId(addr, 0);
    raftNode_ = std::make_shared<Node>(groupId, peerId_);
    concurrentapply_ = options.concurrentapply;

    /*
     * 初始化copyset性能metrics
     */
    int ret = ChunkServerMetric::GetInstance()->CreateCopysetMetric(
        logicPoolId_, copysetId_);
    if (ret != 0) {
        LOG(ERROR) << "create copyset metric failed, "
                   << "logicPool id : " <<  logicPoolId_
                   << ", copyset id : " << copysetId_;
        return -1;
    }
    metric_ = ChunkServerMetric::GetInstance()->GetCopysetMetric(
        logicPoolId_, copysetId_);
    if (metric_ != nullptr) {
        // TODO(yyk) 后续考虑添加datastore层面的io metric
        metric_->MonitorDataStore(dataStore_.get());
    }

    return 0;
}

int CopysetNode::Run() {
    // raft node的初始化实际上让起run起来
    if (0 != raftNode_->init(nodeOptions_)) {
        LOG(ERROR) << "Fail to init raft node "
                   << ToGroupIdString(logicPoolId_, copysetId_);
        return -1;
    }
    LOG(INFO) << "init copyset:(" << logicPoolId_ << ", " << copysetId_ << ") "
              << "success";
    return 0;
}

void CopysetNode::Fini() {
    if (nullptr != raftNode_) {
        // 关闭所有关于此raft node的服务
        raftNode_->shutdown(nullptr);
        // 等待所有的正在处理的task结束
        raftNode_->join();
    }
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
            auto task = std::bind(&ChunkOpRequest::OnApply,
                                  opRequest,
                                  iter.index(),
                                  doneGuard.release());
            concurrentapply_->Push(opRequest->ChunkId(), task);
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
            auto opReq = ChunkOpRequest::Decode(log, &request, &data);
            auto task = std::bind(&ChunkOpRequest::OnApplyFromLog,
                                  opReq,
                                  dataStore_,
                                  request,
                                  data);
            concurrentapply_->Push(request.chunkid(), task);
        }
    }
}

void CopysetNode::on_shutdown() {
    LOG(INFO) << ToGroupIdString(logicPoolId_, copysetId_) << ") is shutdown";
}

void CopysetNode::on_snapshot_save(::braft::SnapshotWriter *writer,
                                   ::braft::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    /**
     * 1.flush I/O to disk，确保数据都落盘
     */
    concurrentapply_->Flush();

    /**
     * 2.保存配置版本: conf.epoch，注意conf.epoch是存放在data目录下
     */
    std::string
        filePathTemp = writer->get_path() + "/" + kCurveConfEpochFilename;
    if (0 != SaveConfEpoch(filePathTemp)) {
        done->status().set_error(errno, "invalid: %s", strerror(errno));
        LOG(ERROR) << "SaveConfEpoch failed, "
                   << "errno: " << errno << ", "
                   << "error message: " << strerror(errno);
        return;
    }

    /**
     * 3.保存chunk文件名的列表到快照元数据文件中
     */
    std::vector<std::string> files;
    if (0 == fs_->List(chunkDataApath_, &files)) {
        for (auto it = files.begin(); it != files.end(); ++it) {
            std::string filename;
            // 不是conf epoch文件，保存绝对路径和相对路径
            // 1. 添加绝对路径
            filename.append(chunkDataApath_);
            filename.append("/").append(*it);
            // 2. 添加分隔符
            filename.append(":");
            // 3. 添加相对路径
            filename.append(chunkDataRpath_);
            filename.append("/").append(*it);
            writer->add_file(filename);
        }
    } else {
        done->status().set_error(errno, "invalid: %s", strerror(errno));
        LOG(ERROR) << "dir reader failed, maybe no exist or permission. path "
                   << chunkDataApath_;
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

    // 如果数据目录不存在，那么说明 load snapshot 数据部分就不需要处理
    if (fs_->DirExists(snapshotChunkDataDir)) {
        std::vector<std::string> files;
        if (0 == fs_->List(snapshotChunkDataDir, &files)) {
            for (auto it = files.begin(); it != files.end(); ++it) {
                // /mnt/sda/1-10001/raft_snapshot/snapshot_0043/data/100001.chunk
                std::string snapshotFilename;
                snapshotFilename.append(snapshotChunkDataDir).append("/")
                    .append(*it);
                // /mnt/sda/1-10001/data/100001.chunk
                std::string dataFilename;
                dataFilename.append(chunkDataApath_);
                dataFilename.append("/").append(*it);
                // 这里用RaftSnapshotFilesystemAdaptor提供的rename接口
                // 因为rename原来已经存在的chunk文件会导致原来的chunk文件
                // 无法被chunkfilepool回收，那么chunkfilepool的存量会越来
                // 越少，所以这里采用RaftSnapshotFilesystemAdaptor里的
                // rename接口，在回收之前先检查是否可以回收，如果可以，先回收
                if (false == nodeOptions_.snapshot_file_system_adaptor->get()->
                    rename(snapshotFilename, dataFilename)) {
                    LOG(ERROR) << "rename " << snapshotFilename << " to "
                               << dataFilename << " failed";
                    return -1;
                }
            }
        } else {
            LOG(ERROR) << "dir reader failed, path " << snapshotChunkDataDir
                       << ", error message: " << strerror(errno);
            return -1;
        }
    } else {
        LOG(INFO) << "load snapshot  data path: "
                  << snapshotChunkDataDir << " not exist.";
    }

    /**
     * 2. 加载配置版本文件
     */
    std::string filePath = reader->get_path() + "/" + kCurveConfEpochFilename;
    if (fs_->FileExists(filePath)) {
        if (0 != LoadConfEpoch(filePath)) {
            LOG(ERROR) << "load conf.epoch failed: " << filePath;
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
     * 后面的write的数据就会丢，除此之外，如果 打他store init没有重新open
     * 文件，也将导致read不到恢复过来的数据，而是read到老的数据。
     */
    if (!dataStore_->Initialize()) {
        LOG(ERROR) << "data store init failed in on snapshot load";
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

    return 0;
}

void CopysetNode::on_leader_start(int64_t term) {
    leaderTerm_.store(term, std::memory_order_release);
    ChunkServerMetric::GetInstance()->IncreaseLeaderCount();
    LOG(INFO) << ToGroupIdString(logicPoolId_, copysetId_)
              << ", peer id: " << peerId_.to_string()
              << " become leader, term is: " << leaderTerm_;
}

void CopysetNode::on_leader_stop(const butil::Status &status) {
    leaderTerm_.store(-1, std::memory_order_release);
    ChunkServerMetric::GetInstance()->DecreaseLeaderCount();
    LOG(INFO) << ToGroupIdString(logicPoolId_, copysetId_)
              << ", peer id: " << peerId_.to_string() << " stepped down";
}

void CopysetNode::on_error(const ::braft::Error &e) {
    LOG(ERROR) << ToGroupIdString(logicPoolId_, copysetId_)
               << ", peer id: " << peerId_.to_string()
               << " meet raft error: " << e;
}

void CopysetNode::on_configuration_committed(const Configuration &conf) {
    {
        std::unique_lock<std::mutex> lock_guard(confLock_);
        conf_ = conf;
        epoch_.fetch_add(1, std::memory_order_acq_rel);
    }
    LOG(INFO) << "peer id: " << peerId_.to_string()
              << ", leader id: " << raftNode_->leader_id()
              << ", Configuration of this group is" << conf
              << ", epoch: " << epoch_.load(std::memory_order_acquire);
}

void CopysetNode::on_stop_following(const ::braft::LeaderChangeContext &ctx) {
    LOG(INFO) << ToGroupIdString(logicPoolId_, copysetId_)
              << ", peer id: " << peerId_.to_string()
              << " stops following" << ctx;
}

void CopysetNode::on_start_following(const ::braft::LeaderChangeContext &ctx) {
    LOG(INFO) << ToGroupIdString(logicPoolId_, copysetId_)
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
                       << "(" << loadLogicPoolID << "," << loadCopysetID << ")";
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
    std::unique_lock<std::mutex> lock_guard(confLock_);

    std::vector<PeerId> tempPeers;
    conf_.list_peers(&tempPeers);

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

void CopysetNode::SetCopysetNode(std::shared_ptr<Node> node) {
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

static void DummyFunc(void* arg, const butil::Status& status) {
}

butil::Status CopysetNode::TransferLeader(const Peer& peer) {
    butil::Status status;
    PeerId peerId(peer.address());

    if (raftNode_->leader_id() == peerId) {
        butil::Status status = butil::Status::OK();
        DVLOG(6) << "Skipped transferring leader to leader itself: " << peerId;

        return status;
    }

    int rc = raftNode_->transfer_leadership_to(peerId);
    if (rc != 0) {
        status = butil::Status(rc, "Failed to transfer leader of copyset "
                               "<%u, %u> to peer %s, error: %s",
                               logicPoolId_, copysetId_,
                               peerId.to_string().c_str(), berror(rc));
        LOG(ERROR) << status.error_str();

        return status;
    }

    status = butil::Status::OK();
    LOG(INFO) << "Transferred leader of copyset "
              << ToGroupIdStr(logicPoolId_, copysetId_)
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
                     << ToGroupIdStr(logicPoolId_, copysetId_)
                     << ", skip adding peer";

            return status;
        }
    }

    braft::Closure* addPeerDone = braft::NewCallback(DummyFunc,
            reinterpret_cast<void *>(0));
    raftNode_->add_peer(peerId, addPeerDone);

    return butil::Status::OK();
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
                 << ToGroupIdStr(logicPoolId_, copysetId_) << ", skip removing";

        return status;
    }

    braft::Closure* removePeerDone = braft::NewCallback(DummyFunc,
            reinterpret_cast<void *>(0));
    raftNode_->remove_peer(peerId, removePeerDone);

    return butil::Status::OK();
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

ConcurrentApplyModule *CopysetNode::GetConcurrentApplyModule() const {
    return concurrentapply_;
}

void CopysetNode::Propose(const braft::Task &task) {
    raftNode_->apply(task);
}

int CopysetNode::GetConfChange(ConfigChangeType *type,
                               Configuration *oldConf,
                               Peer *alterPeer) {
    Configuration adding, removing;
    PeerId transferee;

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

    bool ret
        = raftNode_->conf_changes(oldConf, &adding, &removing, &transferee);

    if (false == ret) {
        *type = ConfigChangeType::NONE;
        return 0;
    }

    // 目前仅支持单个成员的配置变更
    if (1 == adding.size()) {
        *type = ConfigChangeType::ADD_PEER;
        alterPeer->set_address(adding.begin()->to_string());
        return 0;
    }

    if (1 == removing.size()) {
        *type = ConfigChangeType::REMOVE_PEER;
        alterPeer->set_address(removing.begin()->to_string());
        return 0;
    }

    if (!transferee.is_empty()) {
        *type = ConfigChangeType::TRANSFER_LEADER;
        alterPeer->set_address(transferee.to_string());
        return 0;
    }

    /*
     * 当前使用braft进行配置变更，仅限一次变更单个成员，所以
     * 如果发现一次变更多个成员，那么认为失败，有问题
     */
    return -1;
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

}  // namespace chunkserver
}  // namespace curve

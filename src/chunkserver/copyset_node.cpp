/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/copyset_node.h"

#include <cassert>

#include "src/chunkserver/chunk_closure.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_storage.h"
#include "src/chunkserver/chunkserverStorage/adaptor_util.h"

namespace curve {
namespace chunkserver {

CopysetNode::CopysetNode(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId,
                         const Configuration &initConf) :
    copysetNodeManager_(&CopysetNodeManager::GetInstance()),
    logicPoolId_(logicPoolId),
    copysetId_(copysetId),
    initConf_(initConf),
    peerId_(),
    nodeOptions_(),
    raftNode_(nullptr),
    filesystemProtocol_("local"),
    chunkDataApath_(),
    chunkDataRpath_(),
    chunkSnapshotUri_(),
    leaderTerm_(-1) {}

CopysetNode::~CopysetNode() {
}

int CopysetNode::Init(const CopysetNodeOptions &options) {
    std::string groupId = ToGroupId(logicPoolId_, copysetId_);

    std::string copiedUri(options.chunkDataUri);
    std::string chunkDataDir;
    std::string protocol = FsAdaptorUtil::ParserUri(copiedUri, &chunkDataDir);
    if (protocol.empty()) {
        LOG(ERROR) << "不存在指定 chunk data uri 的 protocol";
        return -1;
    }

    copysetNodeManager_ = options.copysetNodeManager;

    // Init copyset node 关于 chunk server 的配置，这两个的初始化必须在 raftNode_.init 之前
    filesystemProtocol_ = protocol;
//    chunkDataApath_.append(chunkDataDir).append("/").append(groupId).append("/data");
    chunkDataApath_.append(chunkDataDir).append("/").append(groupId);
    std::shared_ptr<CSSfsAdaptor> sfsAdaptor = ChunkserverStorage::CreateFsAdaptor("", options.chunkDataUri);
    dataStore_ = std::make_unique<CSDataStore>(sfsAdaptor, chunkDataApath_);
    chunkDataApath_.append("/data");
    if (true != dataStore_->Initialize()) {
        LOG(ERROR) << "dataStore " << chunkDataApath_ << " init" << "failed";
        return -1;
    }

    chunkDataRpath_ = "data";       // chunk file 所存放的相对目录
    fs_ = std::make_unique<PosixFileSystemAdaptor>();  // TODO(wudemiao) 后期修改以适应不同的文件系统

    // Init copyset 对应的 raft node options
    nodeOptions_.initial_conf = initConf_;
    nodeOptions_.election_timeout_ms = options.electionTimeoutMs;
    nodeOptions_.fsm = this;
    nodeOptions_.node_owns_fsm = false;
    nodeOptions_.snapshot_interval_s = options.snapshotIntervalS;
    nodeOptions_.log_uri = options.logUri;
    nodeOptions_.log_uri.append("/").append(groupId).append("/log");
    nodeOptions_.raft_meta_uri = options.raftMetaUri;
    nodeOptions_.raft_meta_uri.append("/").append(groupId).append("/raft_meta");
    nodeOptions_.snapshot_uri = options.raftSnapshotUri;
    nodeOptions_.snapshot_uri.append("/").append(groupId).append("/raft_snapshot");
    nodeOptions_.disable_cli = options.disableCli;
    nodeOptions_.usercode_in_pthread = options.usercodeInPthread;

    // 初始化 peer id
    butil::ip_t ip;
    butil::str2ip(options.ip.c_str(), &ip);
    butil::EndPoint addr(ip, options.port);
    // idx 默认是零，在 chunkserver 不允许一个进程有同一个个 copyset 的多副本，这一点注意和 不让braft区别开来
    peerId_ = PeerId(addr, 0);

    // 创建 raft node
    raftNode_ = std::make_shared<Node>(groupId, peerId_);

    return 0;
}

int CopysetNode::Run() {
    if (0 != raftNode_->init(nodeOptions_)) {
        LOG(ERROR) << "Fail to init raft node (" << logicPoolId_ << ", " << copysetId_ << ")";
        return -1;
    }

    return 0;
}

void CopysetNode::Fini() {
    if (nullptr != raftNode_) {
        // 关闭所有关于此 raft node 的服务
        raftNode_->shutdown(nullptr);
        // 等待所有的正在处理的 task 结束
        raftNode_->join();
    }
    if (nullptr != dataStore_) {
        dataStore_->UnInitialize();
    }
}

// TODO(wudemiao): dirty read，也就是 Follower read，根据 request 的 committed index 返回读
void CopysetNode::ReadChunk(::google::protobuf::RpcController *controller,
                            const ChunkRequest *request,
                            ChunkResponse *response,
                            google::protobuf::Closure *done) {
    ApplyChunkRequest(controller, request, response, done);
}

void CopysetNode::DeleteChunk(::google::protobuf::RpcController *controller,
                              const ChunkRequest *request,
                              ChunkResponse *response,
                              google::protobuf::Closure *done) {
    ApplyChunkRequest(controller, request, response, done);
}

void CopysetNode::WriteChunk(::google::protobuf::RpcController *controller,
                             const ChunkRequest *request,
                             ChunkResponse *response,
                             google::protobuf::Closure *done) {
    ApplyChunkRequest(controller, request, response, done);
}

void CopysetNode::CreateChunkSnapshot(::google::protobuf::RpcController *controller,
                                      const ChunkSnapshotRequest *request,
                                      ChunkSnapshotResponse *response,
                                      google::protobuf::Closure *done) {
    ApplyChunkSnapshotRequest(controller, request, response, done);
}

void CopysetNode::DeleteChunkSnapshot(::google::protobuf::RpcController *controller,
                                      const ChunkSnapshotRequest *request,
                                      ChunkSnapshotResponse *response,
                                      google::protobuf::Closure *done) {
    ApplyChunkSnapshotRequest(controller, request, response, done);
}

void CopysetNode::ReadChunkSnapshot(::google::protobuf::RpcController *controller,
                                    const ChunkSnapshotRequest *request,
                                    ChunkSnapshotResponse *response,
                                    google::protobuf::Closure *done) {
    ApplyChunkSnapshotRequest(controller, request, response, done);
}

void CopysetNode::on_apply(::braft::Iterator &iter) {
    for (; iter.valid(); iter.next()) {
        // 放在 bthread 中异步执行，避免阻塞当前状态机的执行
        braft::AsyncClosureGuard doneGuard(iter.done());

        // 解析 log entry 的 data 部分
        butil::IOBuf data = iter.data();
        RequestType type = RequestType::UNKNOWN_OP;
        data.cutn(&type, sizeof(uint8_t));

        ChunkClosure *chunkClosure = nullptr;
        ChunkSnapshotClosure *snapshotClosure = nullptr;

        switch (type) {
            // chunk op
            case RequestType::CHUNK_OP:
                if (nullptr != iter.done()) {
                    chunkClosure = dynamic_cast<ChunkClosure *>(iter.done());
                    assert(nullptr != chunkClosure);
                    ChunkOpRequest *chunkOpRequest = chunkClosure->GetOpRequest();
                    if (0 == chunkOpRequest->OnApply(shared_from_this())) {
                        ChunkResponse *response = chunkOpRequest->GetResponse();
                        response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
                    } else {
                        LOG(ERROR) << "chunk op apply failed";
                    }
                } else {
                    ChunkOpRequest::OnApply(shared_from_this(), &data);  // 不用返回 rpc
                }
                break;
                // chunk snapshot op
            case RequestType::CHUNK_SNAPSHOT_OP:
                if (nullptr != iter.done()) {
                    snapshotClosure = dynamic_cast<ChunkSnapshotClosure *>(iter.done());
                    assert(nullptr != snapshotClosure);
                    ChunkSnapshotOpRequest *opRequest = snapshotClosure->GetOpRequest();
                    opRequest->OnApply(shared_from_this());
                    opRequest->GetResponse()->set_status(
                        CHUNK_SNAPSHOT_OP_STATUS::CHUNK_SNAPSHOT_OP_STATUS_SUCCESS);
                } else {
                    ChunkSnapshotOpRequest::OnApply(shared_from_this(), &data);  // 不用返回 rpc
                }
                break;
            default:
                LOG(FATAL) << "Unknown Op type";
                break;
        }
    }
}

void CopysetNode::on_shutdown() {
    LOG(INFO) << "Node (" << logicPoolId_ << ", " << copysetId_ << ") is shutdown";
}

// TODO(wudemiao): 快速实现，仅仅实现 data 部分，snapshot 后面再添加
void CopysetNode::on_snapshot_save(::braft::SnapshotWriter *writer, ::braft::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    // /var/mnt/sda/1-10001/data
    if (!fs_->path_exists(chunkDataApath_)) {  // chunk data dir 应该是在 chunk storage 初始化的时候创建
        LOG(WARNING) << "path not exist, path " << chunkDataApath_;
        return;
    }

    std::unique_ptr<DirReader> dirReader(fs_->directory_reader(chunkDataApath_));
    if (dirReader->is_valid()) {
        while (dirReader->next()) {
            // /var/mnt/sda/1-10001/data/100001.chunk:data/100001.chunk
            // 1. 添加绝对路径
            std::string filename;
            filename.append(chunkDataApath_).append("/").append(dirReader->name());
            // 2. 添加分隔符
            filename.append(":");
            // 3. 添加相对路径
            filename.append(chunkDataRpath_).append("/").append(dirReader->name());
            writer->add_file(filename);
        }
    } else {
        LOG(FATAL) << "dir reader failed, maybe no exist or permission. path " << chunkDataApath_;
    }
}

int CopysetNode::on_snapshot_load(::braft::SnapshotReader *reader) {
    int ret = 0;

    // 打开的 snapshot path: /var/mnt/sda/1-10001/raft_snapshot/snapshot_0043
    std::string snapshotPath = reader->get_path();

    // /var/mnt/sda/1-10001/raft_snapshot/snapshot_0043/data
    std::string snapshotChunkDataDir;
    snapshotChunkDataDir.append(snapshotPath).append("/").append(chunkDataRpath_);
    if (!fs_->path_exists(snapshotChunkDataDir)) {
        LOG(INFO) << "path not exist, path " << snapshotChunkDataDir;
        return 0;
    }

    std::unique_ptr<DirReader> dirReader(fs_->directory_reader(snapshotChunkDataDir));
    if (dirReader->is_valid()) {
        while (dirReader->next()) {
            // /var/mnt/sda/1-10001/raft_snapshot/snapshot_0043/data/100001.chunk
            std::string snapshotFilename;
            snapshotFilename.append(snapshotChunkDataDir).append("/").append(dirReader->name());
            // /var/mnt/sda/1-10001/data/100001.chunk
            std::string dataFilename;
            dataFilename.append(chunkDataApath_).append("/").append(dirReader->name());
            if (!fs_->rename(snapshotFilename, dataFilename)) {
                LOG(ERROR) << "rename " << snapshotFilename << " to " << dataFilename << " failed";
                ret = -1;
                break;
            }
        }
    } else {
        LOG(ERROR) << "dir reader failed, maybe no exist or permission. path " << snapshotPath;
        ret = -1;
    }

    return ret;
}

void CopysetNode::on_leader_start(int64_t term) {
    leaderTerm_.store(term, std::memory_order_release);
    LOG(INFO) << "Node (" << logicPoolId_ << ", " << copysetId_ << ") become leader, term: " << leaderTerm_;
}

void CopysetNode::on_leader_stop(const butil::Status &status) {
    leaderTerm_.store(-1, std::memory_order_release);
    LOG(INFO) << "Node (" << logicPoolId_ << ", " << copysetId_ << ") stepped down";
}

void CopysetNode::on_error(const ::braft::Error &e) {
    LOG(ERROR) << "Node (" << logicPoolId_ << ", " << copysetId_ << ") meet raft error " << e;
}

void CopysetNode::on_configuration_committed(const ::braft::Configuration &conf) {
    LOG(INFO) << "Configuration of this group is" << conf;
}

void CopysetNode::on_stop_following(const ::braft::LeaderChangeContext &ctx) {
    LOG(INFO) << "Node (" << logicPoolId_ << ", " << copysetId_ << ")  stops following" << ctx;
}

void CopysetNode::on_start_following(const ::braft::LeaderChangeContext &ctx) {
    LOG(INFO) << "Node (" << logicPoolId_ << ", " << copysetId_ << ")  start following" << ctx;
}

void CopysetNode::RedirectChunkRequest(ChunkResponse *response) {
    CHECK(nullptr != raftNode_);
    PeerId leader = raftNode_->leader_id();
    if (!leader.is_empty()) {
        response->set_redirect(leader.to_string());
    }
    response->set_status(CHUNK_OP_STATUS::CHUNK_OP_STATUS_REDIRECTED);
}

void CopysetNode::RedirectChunkSnapshotRequest(ChunkSnapshotResponse *response) {
    response->set_status(CHUNK_SNAPSHOT_OP_STATUS::CHUNK_SNAPSHOT_OP_STATUS_REDIRECTED);
    if (nullptr != raftNode_) {
        PeerId leader = raftNode_->leader_id();
        if (!leader.is_empty()) {
            response->set_redirect(leader.to_string());
        }
    }
}

void CopysetNode::ApplyChunkRequest(::google::protobuf::RpcController *controller,
                                    const ChunkRequest *request,
                                    ChunkResponse *response,
                                    google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    // 检查任期和自己是不是 Leader
    const int64_t term = leaderTerm_.load(std::memory_order_acquire);
    if (0 > term ||
        0 != strcmp(peerId_.to_string().c_str(),
                    raftNode_->leader_id().to_string().c_str())) {
        RedirectChunkRequest(response);
        return;
    }

    // 打包 op 为 task
    ChunkOpRequest *req = new ChunkOpRequest(copysetNodeManager_, controller, request, response, doneGuard.release());
    braft::Task task;
    butil::IOBuf log;
    if (0 != req->Encode(&log)) {
        LOG(ERROR) << "chunk op request encode failure";
        return;             // rpc response 已经在 Encode 内部设置
    }
    task.data = &log;
    task.done = new ChunkClosure(this, req);

    // apply task to raft node process
    return raftNode_->apply(task);
}

void CopysetNode::ApplyChunkSnapshotRequest(::google::protobuf::RpcController *controller,
                                            const ChunkSnapshotRequest *request,
                                            ChunkSnapshotResponse *response,
                                            google::protobuf::Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    // 检查任期和自己是不是 Leader
    const int64_t term = leaderTerm_.load(std::memory_order_acquire);
    if (0 > term ||
        0 != strcmp(peerId_.to_string().c_str(),
                    raftNode_->leader_id().to_string().c_str())) {
        RedirectChunkSnapshotRequest(response);
        return;
    }

    // 打包 op 为 task
    ChunkSnapshotOpRequest
        *req = new ChunkSnapshotOpRequest(copysetNodeManager_, controller, request, response, doneGuard.release());
    braft::Task task;
    if (0 != req->Encode(task.data)) {
        LOG(ERROR) << "chunk snapshot op request encode failure";
        return;             // rpc response 已经在 Encode 内部设置
    }
    task.done = new ChunkSnapshotClosure(this, req);

    // apply task to raft node process
    return raftNode_->apply(task);
}

CopysetNodeOptions::CopysetNodeOptions()
    : electionTimeoutMs(1000),
      snapshotIntervalS(3600),
      catchupMargin(1000),
      usercodeInPthread(false),
      disableCli(false),
      logUri("/log"),
      raftMetaUri("/raft_meta"),
      raftSnapshotUri("/raft_snapshot"),
      chunkDataUri("/data"),
      chunkSnapshotUri("/snapshot"),
      port(8200),
      maxChunkSize(4 * 1024 * 1024),
      copysetNodeManager(nullptr) {
}
CopysetNodeOptions::CopysetNodeOptions(const CopysetNodeOptions &copysetNodeOptions)
    : electionTimeoutMs(copysetNodeOptions.electionTimeoutMs),
      snapshotIntervalS(copysetNodeOptions.electionTimeoutMs),
      catchupMargin(copysetNodeOptions.catchupMargin),
      usercodeInPthread(copysetNodeOptions.usercodeInPthread),
      disableCli(copysetNodeOptions.disableCli),
      logUri(copysetNodeOptions.logUri),
      raftMetaUri(copysetNodeOptions.raftMetaUri),
      raftSnapshotUri(copysetNodeOptions.raftSnapshotUri),
      chunkDataUri(copysetNodeOptions.chunkDataUri),
      chunkSnapshotUri(copysetNodeOptions.chunkSnapshotUri),
      ip(copysetNodeOptions.ip),
      port(copysetNodeOptions.port),
      maxChunkSize(copysetNodeOptions.maxChunkSize),
      copysetNodeManager(copysetNodeOptions.copysetNodeManager) {
}

CopysetNodeOptions &CopysetNodeOptions::operator=(const CopysetNodeOptions &copysetNodeOptions) {
    electionTimeoutMs = copysetNodeOptions.electionTimeoutMs;
    snapshotIntervalS = copysetNodeOptions.snapshotIntervalS;
    catchupMargin = copysetNodeOptions.catchupMargin;
    usercodeInPthread = copysetNodeOptions.usercodeInPthread;
    disableCli = copysetNodeOptions.disableCli;
    logUri = copysetNodeOptions.logUri;
    raftMetaUri = copysetNodeOptions.raftMetaUri;
    raftSnapshotUri = copysetNodeOptions.raftSnapshotUri;
    chunkDataUri = copysetNodeOptions.chunkDataUri;
    chunkSnapshotUri = copysetNodeOptions.chunkSnapshotUri;
    ip = copysetNodeOptions.ip;
    port = copysetNodeOptions.port;
    maxChunkSize = copysetNodeOptions.maxChunkSize;
    copysetNodeManager = copysetNodeOptions.copysetNodeManager;
    return *this;
}

}  // namespace chunkserver
}  // namespace curve

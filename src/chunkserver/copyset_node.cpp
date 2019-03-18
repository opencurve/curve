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

#include <cassert>

#include "src/chunkserver/chunk_closure.h"
#include "src/chunkserver/op_request.h"
#include "src/fs/fs_common.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/datastore/define.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"

namespace curve {
namespace chunkserver {

const char *kCurveConfEpochFilename = "conf.epoch";

CopysetNode::CopysetNode(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId,
                         const Configuration &initConf) :
    logicPoolId_(logicPoolId),
    copysetId_(copysetId),
    initConf_(initConf),
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
}

int CopysetNode::Init(const CopysetNodeOptions &options) {
    std::string groupId = ToGroupId(logicPoolId_, copysetId_);

    std::string chunkDataDir;
    std::string protocol = FsAdaptorUtil::ParserUri(options.chunkDataUri,
                                                    &chunkDataDir);
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
    chunkDataApath_.append(chunkDataDir).append("/").append(groupId);
    fs_ = options.localFileSystem;
    CHECK(nullptr != fs_) << "local file sytem is null";
    epochFile_ = std::make_unique<ConfEpochFile>(fs_);

    chunkDataRpath_ = "data";
    chunkDataApath_.append("/data");
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

    /**
     * Init copyset对应的raft node options
     */
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
    nodeOptions_.snapshot_uri.append("/")
        .append(groupId).append("/raft_snapshot");
    nodeOptions_.usercode_in_pthread = options.usercodeInPthread;

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
    std::string filePath = chunkDataApath_ + "/" + kCurveConfEpochFilename;
    if (0 != fs_->Rename(filePathTemp, filePath)) {
        done->status().set_error(errno, "invalid: %s", strerror(errno));
        LOG(ERROR) << "rename conf epoch failed, "
                   << filePathTemp << " to " << filePath << ", "
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
            filename.append(chunkDataApath_);
            filename.append("/").append(*it);
            /* 2. 添加分隔符 */
            filename.append(":");
            /* 3. 添加相对路径 */
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
                if (0 != fs_->Rename(snapshotFilename, dataFilename)) {
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
    std::string filePath = chunkDataApath_ + "/" + kCurveConfEpochFilename;
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
        initConf_.reset();
        for (int i = 0; i < meta.peers_size(); ++i) {
            initConf_.add_peer(meta.peers(i));
        }
    }

    return 0;
}

void CopysetNode::on_leader_start(int64_t term) {
    leaderTerm_.store(term, std::memory_order_release);
    LOG(INFO) << ToGroupIdString(logicPoolId_, copysetId_)
              << ", peer id: " << peerId_.to_string()
              << " become leader, term is: " << leaderTerm_;
}

void CopysetNode::on_leader_stop(const butil::Status &status) {
    leaderTerm_.store(-1, std::memory_order_release);
    LOG(INFO) << ToGroupIdString(logicPoolId_, copysetId_)
              << ", peer id: " << peerId_.to_string() << " stepped down";
}

void CopysetNode::on_error(const ::braft::Error &e) {
    LOG(ERROR) << ToGroupIdString(logicPoolId_, copysetId_)
               << ", peer id: " << peerId_.to_string()
               << " meet raft error: " << e;
}

void CopysetNode::on_configuration_committed(const Configuration &conf) {
    epoch_.fetch_add(1, std::memory_order_acq_rel);
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

void CopysetNode::SetCSDateStore(std::shared_ptr<CSDataStore> datastore) {
    dataStore_ = datastore;
}

void CopysetNode::SetLocalFileSystem(std::shared_ptr<LocalFileSystem> fs) {
    fs_ = fs;
}

void CopysetNode::SetConfEpochFile(std::unique_ptr<ConfEpochFile> epochFile) {
    epochFile_ = std::move(epochFile);
}

bool CopysetNode::IsLeaderTerm() const {
    if (0 < leaderTerm_.load(std::memory_order_acquire))
        return true;
    return false;
}

PeerId CopysetNode::GetLeaderId() const {
    return raftNode_->leader_id();
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

}  // namespace chunkserver
}  // namespace curve

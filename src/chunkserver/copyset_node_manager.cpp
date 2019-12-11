/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/copyset_node_manager.h"

#include <glog/logging.h>
#include <braft/file_service.h>
#include <braft/node_manager.h>

#include <vector>
#include <string>
#include <utility>

#include "src/common/string_util.h"
#include "src/common/timeutility.h"
#include "src/chunkserver/chunk_service.h"
#include "src/chunkserver/op_request.h"
#include "src/chunkserver/copyset_service.h"
#include "src/chunkserver/braft_cli_service.h"
#include "src/chunkserver/braft_cli_service2.h"
#include "src/chunkserver/uri_paser.h"


namespace curve {
namespace chunkserver {

using curve::common::TimeUtility;

std::once_flag addServiceFlag;

int CopysetNodeManager::Init(const CopysetNodeOptions &copysetNodeOptions) {
    copysetNodeOptions_ = copysetNodeOptions;
    if (copysetNodeOptions_.loadConcurrency > 0) {
        copysetLoader_ = std::make_shared<TaskThreadPool>();
    } else {
        copysetLoader_ = nullptr;
    }
    return 0;
}

int CopysetNodeManager::Run() {
    if (running_.exchange(true, std::memory_order_acq_rel)) {
        return 0;
    }

    int ret = 0;
    // 启动线程池
    if (copysetLoader_ != nullptr) {
        ret = copysetLoader_->Start(
            copysetNodeOptions_.loadConcurrency);
        if (ret < 0) {
            LOG(ERROR) << "CopysetLoadThrottle start error. ThreadNum: "
                       << copysetNodeOptions_.loadConcurrency;
            return -1;
        }
    }

    // 启动加载已有的copyset
    return ReloadCopysets();
}

int CopysetNodeManager::Fini() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return 0;
    }

    if (copysetLoader_ != nullptr) {
        copysetLoader_->Stop();
        copysetLoader_ = nullptr;
    }

    for (auto& copysetNode : copysetNodeMap_) {
        copysetNode.second->Fini();
    }
    copysetNodeMap_.clear();
    return 0;
}

int CopysetNodeManager::ReloadCopysets() {
    std::string datadir =
        UriParser::GetPathFromUri(copysetNodeOptions_.chunkDataUri);
    if (!copysetNodeOptions_.localFileSystem->DirExists(datadir)) {
        LOG(INFO) << datadir << " not exist. copysets was never created";
        return 0;
    }

    vector<std::string> items;
    if (copysetNodeOptions_.localFileSystem->List(datadir, &items) != 0) {
        LOG(ERROR) << "Failed to get copyset list from data directory "
                   << datadir;
        return -1;
    }

    vector<std::string>::iterator it = items.begin();
    for (; it != items.end(); ++it) {
        LOG(INFO) << "Found copyset dir " << *it;

        uint64_t groupId;
        if (false == ::curve::common::StringToUll(*it, &groupId)) {
            LOG(ERROR) << "parse " << *it << " to graoupId err";
            return -1;
        }
        uint64_t poolId = GetPoolID(groupId);
        uint64_t copysetId = GetCopysetID(groupId);
        LOG(INFO) << "Parsed groupid " << groupId
                  << " as " << ToGroupIdStr(poolId, copysetId);

        if (copysetLoader_ == nullptr) {
            LoadCopyset(poolId, copysetId, false);
        } else {
            copysetLoader_->Enqueue(
                std::bind(&CopysetNodeManager::LoadCopyset,
                          this,
                          poolId,
                          copysetId,
                          true));
        }
    }

    // 如果加载成功，则等待所有copyset加载完成，关闭线程池
    if (copysetLoader_ != nullptr) {
        while (copysetLoader_->QueueSize() != 0) {
            ::sleep(1);
        }
        // queue size为0，但是线程池中的线程仍然可能还在执行
        // stop内部会去join thread，以此保证所有任务执行完以后再退出
        copysetLoader_->Stop();
        copysetLoader_ = nullptr;
    }

    return 0;
}

void CopysetNodeManager::LoadCopyset(const LogicPoolID &logicPoolId,
                                     const CopysetID &copysetId,
                                     bool needCheckLoadFinished) {
    LOG(INFO) << "Begin to load copyset: <"
              << logicPoolId << "," << copysetId << ">, "
              << "check load finished?: "
              << (needCheckLoadFinished ? "Yes." : "No.");

    uint64_t beginTime = TimeUtility::GetTimeofDayMs();
    std::vector<Peer> peers;
    if (!CreateCopysetNode(logicPoolId, copysetId, peers)) {
        LOG(ERROR) << "Failed to load copyset: <"
                   << logicPoolId << "," << copysetId << ">";
        return;
    }
    if (needCheckLoadFinished) {
        std::shared_ptr<CopysetNode> node =
            GetCopysetNode(logicPoolId, copysetId);
        CheckCopysetUntilLoadFinished(node);
    }
    LOG(INFO) << "Load copyset: <" << logicPoolId
              << "," << copysetId << "> end, time used (ms): "
              <<  TimeUtility::GetTimeofDayMs() - beginTime;
}

bool CopysetNodeManager::CheckCopysetUntilLoadFinished(
    std::shared_ptr<CopysetNode> node) {
    if (node == nullptr) {
        LOG(WARNING) << "CopysetNode ptr is null.";
        return false;
    }
    uint32_t retryTimes = 0;

    while (retryTimes < copysetNodeOptions_.checkRetryTimes) {
        if (!running_.load(std::memory_order_acquire)) {
            return false;
        }
        int64_t leaderCommittedIndex = node->GetLeaderCommittedIndex();
        // 小于0一般还没选出leader或者leader心跳还未发送到当前节点
        // 正常通过几次重试可以获取到leader信息，如果重试多次都未获取到
        // 则认为copyset当前可能无法选出leader，直接退出
        if (leaderCommittedIndex < 0) {
            ++retryTimes;
            ::usleep(1000 * copysetNodeOptions_.electionTimeoutMs);
            continue;
        }
        NodeStatus status;
        node->GetStatus(&status);
        int64_t margin = leaderCommittedIndex - status.known_applied_index;
        if (margin < (int64_t)copysetNodeOptions_.finishLoadMargin) {
            LOG(INFO) << "Load copyset: <" << node->GetLogicPoolId()
                      << "," << node->GetCopysetId() << "> finished, "
                      << "leader CommittedIndex: " << leaderCommittedIndex
                      << ", node appliedIndex: " << status.known_applied_index;
            return true;
        }
        retryTimes = 0;
        ::usleep(1000 * copysetNodeOptions_.checkLoadMarginIntervalMs);
    }
    LOG(WARNING) << "check copyset: <" << node->GetLogicPoolId()
                 << "," << node->GetCopysetId() << "> failed.";
    return false;
}

std::shared_ptr<CopysetNode> CopysetNodeManager::GetCopysetNode(
    const LogicPoolID &logicPoolId, const CopysetID &copysetId) const {
    /* 加读锁 */
    ReadLockGuard readLockGuard(rwLock_);
    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    auto it = copysetNodeMap_.find(groupId);
    if (copysetNodeMap_.end() != it)
        return it->second;

    return nullptr;
}

void CopysetNodeManager::GetAllCopysetNodes(
    std::vector<CopysetNodePtr> *nodes) const {
    /* 加读锁 */
    ReadLockGuard readLockGuard(rwLock_);
    for (auto it = copysetNodeMap_.begin(); it != copysetNodeMap_.end(); ++it) {
        nodes->push_back(it->second);
    }
}

bool CopysetNodeManager::CreateCopysetNode(const LogicPoolID &logicPoolId,
                                           const CopysetID &copysetId,
                                           const Configuration &conf) {
    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    /* 加写锁 */
    WriteLockGuard writeLockGuard(rwLock_);
    if (copysetNodeMap_.end() == copysetNodeMap_.find(groupId)) {
        std::shared_ptr<CopysetNode> copysetNode =
            std::make_shared<CopysetNode>(logicPoolId,
                                          copysetId,
                                          conf);
        CHECK(nullptr != copysetNode) << "new copyset node <"
                                      << logicPoolId << ","
                                      << copysetId << "> failed ";
        if (0 != copysetNode->Init(copysetNodeOptions_)) {
            LOG(ERROR) << "Copyset (" << logicPoolId << "," << copysetId << ")"
                       << " init failed";
            return false;
        }
        if (0 != copysetNode->Run()) {
            copysetNode->Fini();
            LOG(ERROR) << "copyset (" << logicPoolId << "," << copysetId << ")"
                       << " run failed";
            return false;
        }
        copysetNodeMap_.insert(std::pair<GroupId, std::shared_ptr<CopysetNode>>(
            groupId,
            copysetNode));

        return true;
    }
    return false;
}

bool CopysetNodeManager::CreateCopysetNode(const LogicPoolID &logicPoolId,
                                           const CopysetID &copysetId,
                                           const std::vector<Peer> peers) {
    GroupId groupId = ToGroupId(logicPoolId, copysetId);

    Configuration conf;
    for (Peer peer : peers) {
        conf.add_peer(PeerId(peer.address()));
    }

    /* 加写锁 */
    WriteLockGuard writeLockGuard(rwLock_);
    if (copysetNodeMap_.end() == copysetNodeMap_.find(groupId)) {
        std::shared_ptr<CopysetNode> copysetNode =
            std::make_shared<CopysetNode>(logicPoolId,
                                          copysetId,
                                          conf);
        CHECK(nullptr != copysetNode) << "new copyset node <"
                                      << logicPoolId << ","
                                      << copysetId << "> failed ";
        if (0 != copysetNode->Init(copysetNodeOptions_)) {
            LOG(ERROR) << "Copyset (" << logicPoolId << "," << copysetId << ")"
                       << " init failed";
            return false;
        }
        if (0 != copysetNode->Run()) {
            copysetNode->Fini();
            LOG(ERROR) << "copyset (" << logicPoolId << "," << copysetId << ")"
                       << " run failed";
            return false;
        }
        copysetNodeMap_.insert(std::pair<GroupId, std::shared_ptr<CopysetNode>>(
            groupId,
            copysetNode));

        return true;
    }
    return false;
}

int CopysetNodeManager::AddService(brpc::Server *server,
                                   const butil::EndPoint &listenAddress) {
    int ret = 0;
    uint64_t maxInflight = 100;
    std::shared_ptr<InflightThrottle> inflightThrottle
        = std::make_shared<InflightThrottle>(maxInflight);
    CHECK(nullptr != inflightThrottle) << "new inflight throttle failed";
    CopysetNodeManager *copysetNodeManager = this;
    ChunkServiceOptions chunkServiceOptions;
    chunkServiceOptions.copysetNodeManager = copysetNodeManager;
    chunkServiceOptions.inflightThrottle = inflightThrottle;

    do {
        if (nullptr == server) {
            LOG(ERROR) << "server is NULL";
            ret = -1;
            break;
        }
        ret = server->AddService(braft::file_service(),
                                 brpc::SERVER_DOESNT_OWN_SERVICE);
        CHECK(0 == ret) << "Fail to add FileService";
        ret = server->AddService(new braft::RaftServiceImpl(listenAddress),
                                 brpc::SERVER_OWNS_SERVICE);
        CHECK(0 == ret) << "Fail to add RaftService";

        ret = server->AddService(new BRaftCliServiceImpl,
                                 brpc::SERVER_OWNS_SERVICE);
        CHECK(0 == ret) << "Fail to add BRaftCliService";

        ret = server->AddService(new BRaftCliServiceImpl2,
                                 brpc::SERVER_OWNS_SERVICE);
        CHECK(0 == ret) << "Fail to add BRaftCliService2";

        ret = server->AddService(new CopysetServiceImpl(copysetNodeManager),
                                 brpc::SERVER_OWNS_SERVICE);
        CHECK(0 == ret) << "Fail to add CopysetService";
        ret = server->AddService(new ChunkServiceImpl(chunkServiceOptions),
                                 brpc::SERVER_OWNS_SERVICE);
        CHECK(0 == ret) << "Fail to add ChunkService";

        if (!braft::NodeManager::GetInstance()->server_exists(listenAddress)) {
            braft::NodeManager::GetInstance()->add_address(listenAddress);
        }
    } while (false);

    return ret;
}

bool CopysetNodeManager::DeleteCopysetNode(const LogicPoolID &logicPoolId,
                                           const CopysetID &copysetId) {
    bool ret = false;
    GroupId groupId = ToGroupId(logicPoolId, copysetId);

    {
        // 加读锁
        ReadLockGuard readLockGuard(rwLock_);
        auto it = copysetNodeMap_.find(groupId);
        if (copysetNodeMap_.end() != it) {
            it->second->Fini();
            ret = true;
        }
    }

    {
        // 加写锁
        WriteLockGuard writeLockGuard(rwLock_);
        auto it = copysetNodeMap_.find(groupId);
        if (copysetNodeMap_.end() != it) {
            copysetNodeMap_.erase(it);
            ret = true;
            LOG(INFO) << "Delete copyset success, groupid: " << groupId;
        }
    }

    return ret;
}

bool CopysetNodeManager::PurgeCopysetNodeData(const LogicPoolID &logicPoolId,
                                              const CopysetID &copysetId) {
    bool ret = false;
    GroupId groupId = ToGroupId(logicPoolId, copysetId);

    {
        // 加读锁
        ReadLockGuard readLockGuard(rwLock_);
        auto it = copysetNodeMap_.find(groupId);
        if (copysetNodeMap_.end() != it) {
            it->second->Fini();
            ret = true;
        }
    }

    {
        // 加写锁
        WriteLockGuard writeLockGuard(rwLock_);
        auto it = copysetNodeMap_.find(groupId);
        if (copysetNodeMap_.end() != it) {
            if (0 != copysetNodeOptions_.trash->RecycleCopySet(
                it->second->GetCopysetDir())) {
                LOG(ERROR) << "Failed to remove copyset <" << logicPoolId
                           << ", " << copysetId << "> persistently";
                ret = false;
            }
            LOG(INFO) << "Move copyset to trash success, groupid: " << groupId;
            copysetNodeMap_.erase(it);
            ret = true;
        }
    }

    return ret;
}

bool CopysetNodeManager::IsExist(const LogicPoolID &logicPoolId,
                                 const CopysetID &copysetId) {
    /* 加读锁 */
    ReadLockGuard readLockGuard(rwLock_);
    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    return copysetNodeMap_.end() != copysetNodeMap_.find(groupId);
}

}  // namespace chunkserver
}  // namespace curve

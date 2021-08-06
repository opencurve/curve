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
#include "src/common/uri_parser.h"
#include "src/chunkserver/raftsnapshot/curve_file_service.h"


namespace curve {
namespace chunkserver {

using curve::common::TimeUtility;

std::once_flag addServiceFlag;

int CopysetNodeManager::Init(const CopysetNodeOptions &copysetNodeOptions) {
    copysetNodeOptions_ = copysetNodeOptions;
    if (copysetNodeOptions_.loadConcurrency > 0) {
        copysetLoader_ = std::make_shared<TaskThreadPool<>>();
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
    ret = ReloadCopysets();
    if (ret == 0) {
        loadFinished_.exchange(true, std::memory_order_acq_rel);
        LOG(INFO) << "Reload copysets success.";
    }
    return ret;
}

int CopysetNodeManager::Fini() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) {
        return 0;
    }
    loadFinished_.exchange(false, std::memory_order_acq_rel);

    if (copysetLoader_ != nullptr) {
        copysetLoader_->Stop();
        copysetLoader_ = nullptr;
    }

    {
        ReadLockGuard readLockGuard(rwLock_);
        for (auto& copysetNode : copysetNodeMap_) {
            copysetNode.second->Fini();
        }
    }

    WriteLockGuard writeLockGuard(rwLock_);
    copysetNodeMap_.clear();

    return 0;
}

int CopysetNodeManager::ReloadCopysets() {
    std::string datadir = curve::common::UriParser::GetPathFromUri(
        copysetNodeOptions_.chunkDataUri);
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
                  << " as " << ToGroupIdString(poolId, copysetId);

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

bool CopysetNodeManager::LoadFinished() {
    return loadFinished_.load(std::memory_order_acquire);
}

void CopysetNodeManager::LoadCopyset(const LogicPoolID &logicPoolId,
                                     const CopysetID &copysetId,
                                     bool needCheckLoadFinished) {
    LOG(INFO) << "Begin to load copyset "
              << ToGroupIdString(logicPoolId, copysetId)
              << ". check load finished? : "
              << (needCheckLoadFinished ? "Yes." : "No.");

    uint64_t beginTime = TimeUtility::GetTimeofDayMs();
    // chunkserver启动加载copyset阶段，会拒绝外部的创建copyset请求
    // 因此不会有其他线程加载或者创建相同copyset，此时不需要加锁
    Configuration conf;
    std::shared_ptr<CopysetNode> copysetNode =
        CreateCopysetNodeUnlocked(logicPoolId, copysetId, conf);
    if (copysetNode == nullptr) {
        LOG(ERROR) << "Failed to create copyset "
                   << ToGroupIdString(logicPoolId, copysetId);
        return;
    }
    if (!InsertCopysetNodeIfNotExist(logicPoolId, copysetId, copysetNode)) {
        LOG(ERROR) << "Failed to insert copyset "
                   << ToGroupIdString(logicPoolId, copysetId);
        return;
    }
    if (needCheckLoadFinished) {
        std::shared_ptr<CopysetNode> node =
            GetCopysetNode(logicPoolId, copysetId);
        CheckCopysetUntilLoadFinished(node);
    }
    LOG(INFO) << "Load copyset " << ToGroupIdString(logicPoolId, copysetId)
              << " end, time used (ms): "
              <<  TimeUtility::GetTimeofDayMs() - beginTime;
}

bool CopysetNodeManager::CheckCopysetUntilLoadFinished(
    std::shared_ptr<CopysetNode> node) {
    if (node == nullptr) {
        LOG(WARNING) << "CopysetNode ptr is null.";
        return false;
    }
    uint32_t retryTimes = 0;
    LogicPoolID logicPoolId = node->GetLogicPoolId();
    CopysetID copysetId = node->GetCopysetId();

    while (retryTimes < copysetNodeOptions_.checkRetryTimes) {
        if (!running_.load(std::memory_order_acquire)) {
            return false;
        }
        NodeStatus leaderStaus;
        bool getSuccess = node->GetLeaderStatus(&leaderStaus);
        // 获取leader状态失败一般是由于还没选出leader或者leader心跳还未发送到当前节点
        // 正常通过几次重试可以获取到leader信息，如果重试多次都未获取到
        // 则认为copyset当前可能无法选出leader，直接退出
        if (!getSuccess) {
            ++retryTimes;
            ::usleep(1000 * copysetNodeOptions_.electionTimeoutMs);
            continue;
        }

        NodeStatus status;
        node->GetStatus(&status);
        // 当前副本的最后一个日志落后于leader上保存的第一个日志
        // 这种情况下此副本会通过安装快照恢复，可以忽略避免阻塞检查线程
        bool mayInstallSnapshot = leaderStaus.first_index > status.last_index;
        if (mayInstallSnapshot) {
            LOG(WARNING) << "Copyset "
                         << ToGroupIdString(logicPoolId, copysetId)
                         << " may installing snapshot, "
                         << "stop checking. "
                         << "fist log index on leader: "
                         << leaderStaus.first_index
                         << ", last log index on current node: "
                         << status.last_index;
            return false;
        }

        // 判断当前副本已经apply的日志是否接近已经committed的日志
        int64_t margin = leaderStaus.committed_index
                       - status.known_applied_index;
        bool catchupLeader = margin
                           < (int64_t)copysetNodeOptions_.finishLoadMargin;
        if (catchupLeader) {
            LOG(INFO) << "Load copyset "
                      << ToGroupIdString(logicPoolId, copysetId)
                      << " finished, "
                      << "leader CommittedIndex: "
                      << leaderStaus.committed_index
                      << ", node appliedIndex: "
                      << status.known_applied_index;
            return true;
        }
        retryTimes = 0;
        ::usleep(1000 * copysetNodeOptions_.checkLoadMarginIntervalMs);
    }
    LOG(WARNING) << "check copyset "
                 << ToGroupIdString(logicPoolId, copysetId)
                 << " failed.";
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
    // 如果本地copyset还未全部加载完成，不允许外部创建copyset
    if (!loadFinished_.load(std::memory_order_acquire)) {
        LOG(WARNING) << "Create copyset failed: load unfinished "
                     << ToGroupIdString(logicPoolId, copysetId);
        return false;
    }
    // copysetnode析构的时候会去调shutdown，可能导致协程切出
    // 所以创建copysetnode失败的时候，不能占着写锁，等写锁释放后再析构
    std::shared_ptr<CopysetNode> copysetNode = nullptr;
    /* 加写锁 */
    WriteLockGuard writeLockGuard(rwLock_);
    if (copysetNodeMap_.end() == copysetNodeMap_.find(groupId)) {
        copysetNode = std::make_shared<CopysetNode>(logicPoolId,
                                                    copysetId,
                                                    conf);
        if (0 != copysetNode->Init(copysetNodeOptions_)) {
            LOG(ERROR) << "Copyset " << ToGroupIdString(logicPoolId, copysetId)
                    << " init failed";
            return false;
        }
        if (0 != copysetNode->Run()) {
            LOG(ERROR) << "Copyset " << ToGroupIdString(logicPoolId, copysetId)
                       << " run failed";
            return false;
        }

        copysetNodeMap_.insert(std::pair<GroupId, std::shared_ptr<CopysetNode>>(
            groupId,
            copysetNode));
        LOG(INFO) << "Create copyset success "
                  << ToGroupIdString(logicPoolId, copysetId);
        return true;
    }
    LOG(WARNING) << "Copyset node is already exists "
                 << ToGroupIdString(logicPoolId, copysetId);
    return false;
}

bool CopysetNodeManager::CreateCopysetNode(const LogicPoolID &logicPoolId,
                                           const CopysetID &copysetId,
                                           const std::vector<Peer> peers) {
    Configuration conf;
    for (Peer peer : peers) {
        conf.add_peer(PeerId(peer.address()));
    }

    return CreateCopysetNode(logicPoolId, copysetId, conf);
}

std::shared_ptr<CopysetNode> CopysetNodeManager::CreateCopysetNodeUnlocked(
    const LogicPoolID &logicPoolId,
    const CopysetID &copysetId,
    const Configuration &conf) {
    std::shared_ptr<CopysetNode> copysetNode =
        std::make_shared<CopysetNode>(logicPoolId,
                                        copysetId,
                                        conf);
    if (0 != copysetNode->Init(copysetNodeOptions_)) {
        LOG(ERROR) << "Copyset " << ToGroupIdString(logicPoolId, copysetId)
                   << " init failed";
        return nullptr;
    }
    if (0 != copysetNode->Run()) {
        copysetNode->Fini();
        LOG(ERROR) << "Copyset " << ToGroupIdString(logicPoolId, copysetId)
                   << " run failed";
        return nullptr;
    }

    return copysetNode;
}

int CopysetNodeManager::AddService(brpc::Server *server,
                                   const butil::EndPoint &listenAddress) {
    int ret = 0;
    uint64_t maxInflight = 100;
    std::shared_ptr<InflightThrottle> inflightThrottle
        = std::make_shared<InflightThrottle>(maxInflight);
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
        // We need call braft::add_service to add endPoint to braft::NodeManager
        braft::add_service(server, listenAddress);
        // We need to replace braft::CliService with our own implementation
        auto service = server->FindServiceByName("CliService");
        ret = server->RemoveService(service);
        CHECK(0 == ret) << "Fail to remove braft::CliService";
        ret = server->AddService(new BRaftCliServiceImpl,
                                 brpc::SERVER_OWNS_SERVICE);
        CHECK(0 == ret) << "Fail to add BRaftCliService";
        // We need to replace braft::FileServiceImpl with our own implementation
        service = server->FindServiceByName("FileService");
        ret = server->RemoveService(service);
        CHECK(0 == ret) << "Fail to remove braft::FileService";
        ret = server->AddService(&kCurveFileService,
        brpc::SERVER_DOESNT_OWN_SERVICE);
        CHECK(0 == ret) << "Fail to add CurveFileService";

        // add other services
        ret = server->AddService(new BRaftCliServiceImpl2,
                                 brpc::SERVER_OWNS_SERVICE);
        CHECK(0 == ret) << "Fail to add BRaftCliService2";

        ret = server->AddService(new CopysetServiceImpl(copysetNodeManager),
                                 brpc::SERVER_OWNS_SERVICE);
        CHECK(0 == ret) << "Fail to add CopysetService";
        ret = server->AddService(new ChunkServiceImpl(chunkServiceOptions),
                                 brpc::SERVER_OWNS_SERVICE);
        CHECK(0 == ret) << "Fail to add ChunkService";
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
            // TODO(yyk) 这部分可能存在死锁的风险，后续需要评估
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
            LOG(INFO) << "Delete copyset "
                      << ToGroupIdString(logicPoolId, copysetId)
                      <<" success.";
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
            // TODO(yyk) 这部分可能存在死锁的风险，后续需要评估
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
                LOG(ERROR) << "Failed to remove copyset "
                           << ToGroupIdString(logicPoolId, copysetId)
                           << " persistently.";
                ret = false;
            }
            LOG(INFO) << "Move copyset"
                      << ToGroupIdString(logicPoolId, copysetId)
                      << "to trash success.";
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

bool CopysetNodeManager::InsertCopysetNodeIfNotExist(
    const LogicPoolID &logicPoolId, const CopysetID &copysetId,
    std::shared_ptr<CopysetNode> node) {
    /* 加写锁 */
    WriteLockGuard writeLockGuard(rwLock_);
    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    auto it = copysetNodeMap_.find(groupId);
    if (copysetNodeMap_.end() == it) {
        copysetNodeMap_.insert(
            std::pair<GroupId, std::shared_ptr<CopysetNode>>(groupId, node));
        LOG(INFO) << "Insert copyset success "
                  << ToGroupIdString(logicPoolId, copysetId);
        return true;
    }
    LOG(WARNING) << "Copyset node is already exists "
                 << ToGroupIdString(logicPoolId, copysetId);
    return false;
}

}  // namespace chunkserver
}  // namespace curve

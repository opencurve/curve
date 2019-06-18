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
#include "src/chunkserver/chunk_service.h"
#include "src/chunkserver/op_request.h"
#include "src/chunkserver/copyset_service.h"
#include "src/chunkserver/braft_cli_service.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_adaptor_util.h"
#include "src/chunkserver/braft_cli_service2.h"


namespace curve {
namespace chunkserver {

std::once_flag addServiceFlag;

int CopysetNodeManager::Init(const CopysetNodeOptions &copysetNodeOptions) {
    copysetNodeOptions_ = copysetNodeOptions;
    return 0;
}

int CopysetNodeManager::Run() {
    /* TODO(wudemiao): 后期有线程池了之后还需要启动线程池等 */
    return 0;
}

int CopysetNodeManager::Fini() {
    return 0;
}

int CopysetNodeManager::ReloadCopysets() {
    std::string datadir =
        FsAdaptorUtil::GetPathFromUri(copysetNodeOptions_.chunkDataUri);
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
        if (*it == "." || *it == "..") {
            continue;
        }

        LOG(INFO) << "Found copyset dir " << *it;

        uint64_t groupId;
        if (false == ::curve::common::StringToUll(*it, &groupId)) {
            LOG(ERROR) << "parse " << *it << " to graoupId err";
            return -1;
        }
        uint64_t poolId = GetPoolID(groupId);
        uint64_t copysetId = GetCopysetID(groupId);
        LOG(INFO) << "Parsed groupid " << groupId
                  << "as " << ToGroupIdStr(poolId, copysetId);

        std::vector<Peer> peers;
        if (!CreateCopysetNode(poolId, copysetId, peers)) {
            LOG(ERROR) << "Failed to recreate copyset: <"
                      << poolId << "," << copysetId << ">";
            return -1;
        }

        LOG(INFO) << "Created copyset: <" << poolId << "," << copysetId << ">";
    }

    return 0;
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

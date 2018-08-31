/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/copyset_node_manager.h"

#include <braft/file_service.h>
#include <braft/node_manager.h>

#include <vector>
#include <utility>

#include "src/chunkserver/braft_cli_service.h"
#include "src/chunkserver/copyset_service.h"
#include "src/chunkserver/chunk_service.h"
#include "src/chunkserver/op_request.h"
#include "src/chunkserver/chunkserverStorage/chunkserver_storage.h"

namespace curve {
namespace chunkserver {

std::once_flag addServiceFlag;

int CopysetNodeManager::Init(const CopysetNodeOptions &copysetNodeOptions) {
    copysetNodeOptions_ = copysetNodeOptions;
    ChunkserverStorage::Init();
    return 0;
}

int CopysetNodeManager::Run() {
    // TODO(wudemiao): 后期有线程池了之后还需要启动线程池等
    return 0;
}

int CopysetNodeManager::Fini() {
    ChunkserverStorage::UnInit();
    return 0;
}

std::shared_ptr<CopysetNode> CopysetNodeManager::GetCopysetNode(const LogicPoolID &logicPoolId,
                                                                const CopysetID &copysetId) const {
    // 加读锁
    std::lock_guard<std::mutex> lock(rwLock_);
    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    auto it = copysetNodeMap_.find(groupId);
    if (copysetNodeMap_.end() != it)
        return it->second;

    return nullptr;
}

void CopysetNodeManager::GetAllCopysetNodes(std::vector<std::shared_ptr<CopysetNode>> *nodes) const {
    // 加读锁
    std::lock_guard<std::mutex> lock(rwLock_);
    for (auto it = copysetNodeMap_.begin(); it != copysetNodeMap_.end(); ++it) {
        nodes->push_back(it->second);
    }
}

// 原本设计，这里应该是根据 request 类型，将 request 分发给后端 Op 处理线程
// 但是目前 rpc 的 bthread 和 Op 的 bthread 都是在同一个线程池，所以这里直接处理
// 后续会隔离两边的线程
void CopysetNodeManager::ScheduleRequest(std::shared_ptr<OpRequest> request) {
    request->Process();
}

bool CopysetNodeManager::CreateCopysetNode(const LogicPoolID &logicPoolId,
                                           const CopysetID &copysetId,
                                           const Configuration &conf) {
    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    // FixMe: 后期采用读写锁，这里加写锁
    std::lock_guard<std::mutex> lock(rwLock_);
    if (copysetNodeMap_.end() == copysetNodeMap_.find(groupId)) {
        std::shared_ptr<CopysetNode> copysetNode = std::make_shared<CopysetNode>(logicPoolId, copysetId, conf);
        if (nullptr == copysetNode) {
            LOG(FATAL) << "new copyset node failed ";
            return false;
        }
        if (0 != copysetNode->Init(copysetNodeOptions_)) {
            LOG(ERROR) << "copyset (" << logicPoolId << "," << copysetId << ")" << " init failed";
            return false;
        }
        if (0 != copysetNode->Run()) {
            copysetNode->Fini();
            LOG(ERROR) << "copyset (" << logicPoolId << "," << copysetId << ")" << " run failed";
            return false;
        }
        copysetNodeMap_.insert(std::pair<GroupId, std::shared_ptr<CopysetNode>>(groupId, copysetNode));

        return true;
    }

    return false;
}

// FixMe: 这个函数需要保证只被执行一次
int CopysetNodeManager::AddService(brpc::Server *server,
                                   const butil::EndPoint &listenAddress) {
    int ret = 0;
    CopysetNodeManager* copysetNodeManager = this;
    ChunkServiceOptions chunkServiceOptions = ChunkServiceOptions{this};

    std::call_once(addServiceFlag, [&]() {
        do {
            if (nullptr == server) {
                LOG(ERROR) << "server is NULL";
                ret = -1;
                break;
            }
            if (0 != server->AddService(braft::file_service(), brpc::SERVER_DOESNT_OWN_SERVICE)) {
                LOG(ERROR) << "Fail to add FileService";
                ret = -1;
                break;
            }
            if (0 != server->AddService(
                new braft::RaftServiceImpl(listenAddress), brpc::SERVER_OWNS_SERVICE)) {
                LOG(ERROR) << "Fail to add RaftService";
                ret = -1;
                break;
            }
            if (0 != server->AddService(new BRaftCliServiceImpl, brpc::SERVER_OWNS_SERVICE)) {
                LOG(ERROR) << "Fail to add BRaftCliService";
                ret = -1;
                break;
            }
            if (0 != server->AddService(new CopysetServiceImpl(copysetNodeManager), brpc::SERVER_OWNS_SERVICE)) {
                LOG(ERROR) << "Fail to add CopysetService";
                ret = -1;
                break;
            }
            if (0 != server->AddService(new ChunkServiceImpl(chunkServiceOptions), brpc::SERVER_OWNS_SERVICE)) {
                LOG(ERROR) << "Fail to add ChunkService";
                ret = -1;
                break;
            }

            if (!braft::NodeManager::GetInstance()->server_exists(listenAddress)) {
                braft::NodeManager::GetInstance()->add_address(listenAddress);
            }
        } while (0);

        return ret;
    });

    return ret;
}

bool CopysetNodeManager::DeleteCopysetNode(const LogicPoolID &logicPoolId, const CopysetID &copysetId) {
    std::lock_guard<std::mutex> lock(rwLock_);
    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    auto it = copysetNodeMap_.find(groupId);
    if (copysetNodeMap_.end() != it) {
        it->second->Fini();
        copysetNodeMap_.erase(it);
        return true;
    }

    return false;
}
bool CopysetNodeManager::IsExist(const LogicPoolID &logicPoolId, const CopysetID &copysetId) {
    GroupId groupId = ToGroupId(logicPoolId, copysetId);
    return copysetNodeMap_.end() != copysetNodeMap_.find(groupId);
}

}  // namespace chunkserver
}  // namespace curve

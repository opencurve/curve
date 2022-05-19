/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Fri Aug  6 17:10:54 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"

#include <brpc/server.h>

#include <string>
#include <utility>

#include "curvefs/src/metaserver/copyset/copyset_reloader.h"
#include "curvefs/src/metaserver/copyset/raft_cli_service2.h"
#include "curvefs/src/metaserver/copyset/utils.h"
#include "src/common/timeutility.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curve::common::TimeUtility;

bool CopysetNodeManager::IsLoadFinished() const {
    return loadFinished_.load(std::memory_order_acquire);
}

bool CopysetNodeManager::DeleteCopysetNodeInternal(PoolId poolId,
                                                   CopysetId copysetId,
                                                   bool removeData) {
    GroupId groupId = ToGroupId(poolId, copysetId);

    // stop copyset node first
    {
        ReadLockGuard lock(lock_);
        auto it = copysets_.find(groupId);
        if (it != copysets_.end()) {
            it->second->Stop();
        } else {
            LOG(WARNING) << "Delete copyset failed, copyset "
                         << ToGroupIdString(poolId, copysetId) << " not found";
            return false;
        }
    }

    // remove copyset node
    {
        WriteLockGuard lock(lock_);
        auto it = copysets_.find(groupId);
        if (it != copysets_.end()) {
            bool ret = true;
            if (removeData) {
                std::string copysetDataDir = it->second->GetCopysetDataDir();
                if (!trash_.RecycleCopyset(copysetDataDir)) {
                    LOG(WARNING) << "Recycle copyset remote data failed, "
                                    "copyset data path: '"
                                 << copysetDataDir << "'";
                    ret = false;
                }
            }

            copysets_.erase(it);
            LOG(INFO) << "Delete copyset " << ToGroupIdString(poolId, copysetId)
                      << " success";
            return ret;
        }
    }

    return false;
}

bool CopysetNodeManager::Init(const CopysetNodeOptions& options) {
    options_ = options;
    return trash_.Init(options_.trashOptions, options_.localFileSystem);
}

bool CopysetNodeManager::Start() {
    if (running_.exchange(true)) {
        return true;
    }

    if (!trash_.Start()) {
        LOG(ERROR) << "Start trash failed";
        return false;
    }

    CopysetReloader reloader(this);
    bool ret = reloader.Init(options_) && reloader.ReloadCopysets();
    if (ret) {
        loadFinished_.store(true, std::memory_order_release);
        LOG(INFO) << "Reload copysets success";
        return true;
    } else {
        running_.store(false, std::memory_order_release);
        LOG(ERROR) << "Reload copysets failed";
        return false;
    }
}

bool CopysetNodeManager::Stop() {
    if (!running_.exchange(false)) {
        LOG(WARNING) << "CopysetNodeManager didn't started";
        return false;
    }

    loadFinished_.store(false);

    {
        ReadLockGuard lock(lock_);
        for (auto& copyset : copysets_) {
            copyset.second->Stop();
        }
    }

    {
        WriteLockGuard lock(lock_);
        copysets_.clear();
    }

    if (!trash_.Stop()) {
        LOG(ERROR) << "Stop trash failed";
        return false;
    }

    LOG(INFO) << "CopysetNodeManager stopped";

    return true;
}

CopysetNode* CopysetNodeManager::GetCopysetNode(PoolId poolId,
                                                CopysetId copysetId) {
    ReadLockGuard lock(lock_);

    auto it = copysets_.find(ToGroupId(poolId, copysetId));
    if (it != copysets_.end()) {
        return it->second.get();
    }

    return nullptr;
}

std::shared_ptr<CopysetNode> CopysetNodeManager::GetSharedCopysetNode(
    PoolId poolId, CopysetId copysetId) {
    ReadLockGuard lock(lock_);

    auto it = copysets_.find(ToGroupId(poolId, copysetId));
    if (it != copysets_.end()) {
        return it->second;
    }

    return nullptr;
}

int CopysetNodeManager::IsCopysetNodeExist(
    const CreateCopysetRequest::Copyset& copyset) {
    ReadLockGuard lock(lock_);
    auto iter = copysets_.find(ToGroupId(copyset.poolid(),
                                         copyset.copysetid()));
    if (iter == copysets_.end()) {
        return 0;
    } else {
        auto copysetNode = iter->second.get();
        std::vector<Peer> peers;
        copysetNode->ListPeers(&peers);
        if (peers.size() != static_cast<size_t>(copyset.peers_size())) {
            return -1;
        }

        for (int i = 0; i < copyset.peers_size(); i++) {
            const auto& cspeer = copyset.peers(i);
            auto iter = std::find_if(peers.begin(), peers.end(),
                                     [&cspeer](const Peer& p) {
                                         return cspeer.address() == p.address();
                                     });
            if (iter == peers.end()) {
                return -1;
            }
        }
    }
    return 1;
}

bool CopysetNodeManager::CreateCopysetNode(PoolId poolId, CopysetId copysetId,
                                           const braft::Configuration& conf,
                                           bool checkLoadFinish) {
    if (checkLoadFinish && !IsLoadFinished()) {
        LOG(WARNING) << "Create copyset " << ToGroupIdString(poolId, copysetId)
                     << " failed, copysets load unfinished";
        return false;
    }

    braft::GroupId groupId = ToGroupId(poolId, copysetId);
    std::shared_ptr<CopysetNode> copysetNode;

    {
        WriteLockGuard lock(lock_);
        if (copysets_.count(groupId) != 0) {
            LOG(WARNING) << "Copyset node already exists: "
                         << ToGroupIdString(poolId, copysetId);
            return false;
        }

        copysetNode =
            std::make_shared<CopysetNode>(poolId, copysetId, conf, this);
        if (!copysetNode->Init(options_)) {
            LOG(ERROR) << "Copyset " << ToGroupIdString(poolId, copysetId)
                       << "init failed";
            return false;
        }

        copysets_.emplace(groupId, copysetNode);
    }

    // node start maybe time-consuming
    if (!copysetNode->Start()) {
        // checkLoadFinish equals to false when reloading copysets after a
        // restart, in this case we should not automaticaly remove copyset's
        // data
        const bool removeData = checkLoadFinish;
        DeleteCopysetNodeInternal(poolId, copysetId, removeData);
        LOG(ERROR) << "Copyset " << ToGroupIdString(poolId, copysetId)
                   << " start failed";
        return false;
    }

    LOG(INFO) << "Create copyset success "
              << ToGroupIdString(poolId, copysetId);
    return true;
}

void CopysetNodeManager::GetAllCopysets(
    std::vector<CopysetNode*>* nodes) const {
    nodes->clear();
    ReadLockGuard lock(lock_);
    for (auto& copyset : copysets_) {
        nodes->push_back(copyset.second.get());
    }
}

void CopysetNodeManager::AddService(brpc::Server* server,
                                    const butil::EndPoint& listenAddr) {
    braft::add_service(server, listenAddr);

    // remove braft CliService and add our implemented cli service
    auto* service = server->FindServiceByName("CliService");
    LOG_IF(FATAL, 0 != server->RemoveService(service));
    LOG_IF(FATAL, 0 != server->AddService(new RaftCliService2(this),
                                          brpc::SERVER_OWNS_SERVICE));
}

bool CopysetNodeManager::DeleteCopysetNode(PoolId poolId, CopysetId copysetId) {
    return DeleteCopysetNodeInternal(poolId, copysetId, false);
}

bool CopysetNodeManager::PurgeCopysetNode(PoolId poolId, CopysetId copysetId) {
    return DeleteCopysetNodeInternal(poolId, copysetId, true);
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

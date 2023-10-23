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

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include <vector>
#include <string>

#include "proto/copyset.pb.h"
#include "src/chunkserver/copyset_service.h"
#include "src/chunkserver/copyset_node_manager.h"

namespace curve {
namespace chunkserver {


void CopysetServiceImpl::CreateCopysetNode(RpcController *controller,
                                           const CopysetRequest *request,
                                           CopysetResponse *response,
                                           Closure *done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller *cntl = dynamic_cast<brpc::Controller *>(controller);

    LOG(INFO) << "Received create copyset request: "
              << ToGroupIdString(request->logicpoolid(), request->copysetid());

    // 解析request中的peers
    Configuration conf;
    for (int i = 0; i < request->peerid_size(); ++i) {
        PeerId peer;
        int ret = peer.parse(request->peerid(i));
        if (ret != 0) {
            cntl->SetFailed(EINVAL,
                            "Fail to parse peer id %s",
                            request->peerid(i).c_str());
            return;
        }
        conf.add_peer(peer);
    }

    LogicPoolID logicPoolID = request->logicpoolid();
    CopysetID copysetID = request->copysetid();
    GroupId groupId = ToGroupId(logicPoolID, copysetID);
    if (false == copysetNodeManager_->IsExist(logicPoolID,
                                              copysetID)) {
        if (true ==
            copysetNodeManager_->CreateCopysetNode(logicPoolID,
                                                   copysetID,
                                                   conf)) {
            response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
        } else {
            response->set_status(
                COPYSET_OP_STATUS::COPYSET_OP_STATUS_FAILURE_UNKNOWN);
        }
    } else {
        response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST);
    }

    LOG(INFO) << "Accomplish create copyset "
              << ToGroupIdString(request->logicpoolid(), request->copysetid())
              << ", response code: "
              << COPYSET_OP_STATUS_Name(response->status());
}

void CopysetServiceImpl::CreateCopysetNode2(RpcController *controller,
                                            const CopysetRequest2 *request,
                                            CopysetResponse2 *response,
                                            Closure *done) {
    (void)controller;
    brpc::ClosureGuard doneGuard(done);
    LOG(INFO) << "Received create copysets request";

    Copyset copyset;
    std::vector<Peer> peers;
    for (int i = 0; i < request->copysets_size(); ++i) {
        copyset = request->copysets(i);
        peers.clear();
        for (int j = 0; j < copyset.peers_size(); ++j) {
            peers.push_back(copyset.peers(j));
        }

        if (true == copysetNodeManager_->IsExist(copyset.logicpoolid(),
                                                 copyset.copysetid())) {
            response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST);
            LOG(WARNING) << "Create copyset "
                         << ToGroupIdString(copyset.logicpoolid(),
                                            copyset.copysetid())
                         << " failed, response code: "
                         << COPYSET_OP_STATUS_Name(COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST); //NOLINT
            return;
        }

        if (false ==
            copysetNodeManager_->CreateCopysetNode(copyset.logicpoolid(),
                                                   copyset.copysetid(),
                                                   peers)) {
            response->set_status(
                COPYSET_OP_STATUS::COPYSET_OP_STATUS_FAILURE_UNKNOWN);
            LOG(ERROR) << "Create copyset "
                       << ToGroupIdString(copyset.logicpoolid(),
                                          copyset.copysetid())
                       << " failed, response code: "
                       << COPYSET_OP_STATUS_Name(COPYSET_OP_STATUS::COPYSET_OP_STATUS_FAILURE_UNKNOWN); //NOLINT
            return;
        }

        LOG(INFO) << "Create copyset "
                  << ToGroupIdString(copyset.logicpoolid(), copyset.copysetid())
                  << " success.";
    }
    response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    LOG(INFO) << "Create " << request->copysets().size() << " copysets success";
}

void CopysetServiceImpl::DeleteBrokenCopyset(RpcController* controller,
                                             const CopysetRequest* request,
                                             CopysetResponse* response,
                                             Closure* done) {
    (void)controller;
    brpc::ClosureGuard doneGuard(done);
    LOG(INFO) << "Receive delete broken copyset request";

    auto poolId = request->logicpoolid();
    auto copysetId = request->copysetid();
    auto groupId = ToGroupIdString(poolId, copysetId);

    // if copyset node exist in the manager means its data is complete
    if (copysetNodeManager_->IsExist(poolId, copysetId)) {
        response->set_status(COPYSET_OP_STATUS_COPYSET_IS_HEALTHY);
        LOG(WARNING) << "Delete broken copyset, " << groupId  << " is healthy";
    } else if (!copysetNodeManager_->DeleteBrokenCopyset(poolId, copysetId)) {
        response->set_status(COPYSET_OP_STATUS_FAILURE_UNKNOWN);
        LOG(ERROR) << "Delete broken copyset " << groupId << " failed";
    } else {
        response->set_status(COPYSET_OP_STATUS_SUCCESS);
        LOG(INFO) << "Delete broken copyset " << groupId << " success";
    }
}

void CopysetServiceImpl::GetCopysetStatus(RpcController *controller,
                                        const CopysetStatusRequest *request,
                                        CopysetStatusResponse *response,
                                        Closure *done) {
    (void)controller;
    brpc::ClosureGuard doneGuard(done);
    LOG(INFO) << "Received GetCopysetStatus request: "
              << ToGroupIdString(request->logicpoolid(), request->copysetid());

    // 判断copyset是否存在
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(
            COPYSET_OP_STATUS::COPYSET_OP_STATUS_COPYSET_NOTEXIST);
        LOG(WARNING) << "GetCopysetStatus failed, copyset node is not found: "
                     << ToGroupIdString(request->logicpoolid(),
                                        request->copysetid());
        return;
    }

    // 获取raft node status
    NodeStatus status;
    nodePtr->GetStatus(&status);
    response->set_state(status.state);
    Peer *peer = new Peer();
    response->set_allocated_peer(peer);
    peer->set_address(status.peer_id.to_string());
    Peer *leader = new Peer();
    response->set_allocated_leader(leader);
    leader->set_address(status.leader_id.to_string());
    response->set_readonly(status.readonly);
    response->set_term(status.term);
    response->set_committedindex(status.committed_index);
    response->set_knownappliedindex(status.known_applied_index);
    response->set_pendingindex(status.pending_index);
    response->set_pendingqueuesize(status.pending_queue_size);
    response->set_applyingindex(status.applying_index);
    response->set_firstindex(status.first_index);
    response->set_lastindex(status.last_index);
    response->set_diskindex(status.disk_index);

    // 获取配置的版本
    response->set_epoch(nodePtr->GetConfEpoch());

    /**
     * 考虑到query hash需要读取copyset的所有chunk数据，然后计算hash值
     * 是一个非常耗时的操作，所以在request会设置query hash字段，如果
     * 为false，那么就不需要查询copyset的hash值
     */
    if (request->queryhash()) {
        std::string hash;
        if (0 != nodePtr->GetHash(&hash)) {
            response->set_status(
                COPYSET_OP_STATUS::COPYSET_OP_STATUS_FAILURE_UNKNOWN);
            LOG(ERROR) << "GetCopysetStatus with get hash failure: "
                       << ToGroupIdString(request->logicpoolid(),
                                          request->copysetid());
            return;
        }

        response->set_hash(hash);
    }

    response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    LOG(INFO) << "GetCopysetStatus success: "
              <<  ToGroupIdString(request->logicpoolid(),
                                  request->copysetid());
}

}  // namespace chunkserver
}  // namespace curve

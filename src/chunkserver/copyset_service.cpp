/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include <vector>
#include <string>

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
    brpc::ClosureGuard doneGuard(done);

    Copyset copyset;
    std::vector<Peer> peers;

    LOG(INFO) << "Received create copysets request";

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
                                            copyset.logicpoolid())
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
                                          copyset.logicpoolid())
                       << " failed, response code: "
                       << COPYSET_OP_STATUS_Name(COPYSET_OP_STATUS::COPYSET_OP_STATUS_FAILURE_UNKNOWN); //NOLINT
            return;
        }

        LOG(INFO) << "Create copyset "
                  << ToGroupIdString(copyset.logicpoolid(),
                                     copyset.logicpoolid())
                  << " success.";
    }

    response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
    LOG(INFO) << "Create " << request->copysets().size() << " copysets success";
}

void CopysetServiceImpl::GetCopysetStatus(RpcController *controller,
                                        const CopysetStatusRequest *request,
                                        CopysetStatusResponse *response,
                                        Closure *done) {
    brpc::ClosureGuard doneGuard(done);

    LOG(INFO) << "Received GetCopysetStatus request: "
              << ToGroupIdString(request->logicpoolid(), request->copysetid());

    // 判断copyset是否存在
    auto nodePtr = copysetNodeManager_->GetCopysetNode(request->logicpoolid(),
                                                       request->copysetid());
    if (nullptr == nodePtr) {
        response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_COPYSET_NOTEXIST);    // NOLINT
        LOG(ERROR) << "GetCopysetStatus failed, copyset node is not found: "
                   << ToGroupIdString(request->logicpoolid(),
                                      request->copysetid());
        return;
    }

    std::string hash;
    if (0 == nodePtr->GetHash(&hash)) {
        response->set_hash(hash);
        response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
        LOG(INFO) << "GetCopysetStatus success: "
                  <<  ToGroupIdString(request->logicpoolid(),
                                      request->copysetid());
        return;
    }

    response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_FAILURE_UNKNOWN);
    LOG(ERROR) << "GetCopysetStatus with unknown failure: "
               << ToGroupIdString(request->logicpoolid(), request->copysetid());
}

}  // namespace chunkserver
}  // namespace curve

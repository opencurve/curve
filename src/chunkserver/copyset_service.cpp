/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

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

}  // namespace chunkserver
}  // namespace curve

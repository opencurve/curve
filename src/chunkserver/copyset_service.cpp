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

    /* 检查 copyset 成员个数，必须 >= 2 */
    if (0 >= request->peerid_size()) {
        cntl->SetFailed(EINVAL,
                        "copyset members must be >= 2，cannot be %d",
                        request->peerid_size());
        return;
    }

    /* 解析 request 中的 peers */
    Configuration conf;
    for (int i = 0; i < request->peerid_size(); ++i) {
        PeerId peer;
        if (peer.parse(request->peerid(i)) != 0) {
            cntl->SetFailed(EINVAL,
                            "Fail to parse peer id %s",
                            request->peerid(i).c_str());
            return;
        }
        conf.add_peer(peer);
    }

    GroupId groupId = ToGroupId(request->logicpoolid(), request->copysetid());
    if (false == copysetNodeManager_->IsExist(request->logicpoolid(),
                                              request->copysetid())) {
        if (true ==
            copysetNodeManager_->CreateCopysetNode(request->logicpoolid(),
                                                   request->copysetid(),
                                                   conf)) {
            response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
        } else {
            response->set_status(
                COPYSET_OP_STATUS::COPYSET_OP_STATUS_FAILURE_UNKNOWN);
        }
    } else {
        response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_EXIST);
    }
}

}  // namespace chunkserver
}  // namespace curve

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
 * Date: Wed Aug 11 14:19:56 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/copyset_service.h"

#include <braft/node.h>
#include <brpc/closure_guard.h>
#include <glog/logging.h>

#include "curvefs/src/metaserver/copyset/utils.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

void CopysetServiceImpl::CreateCopysetNode(
    google::protobuf::RpcController* controller,
    const CreateCopysetRequest* request, CreateCopysetResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);

    LOG(INFO) << "Receive CreateCopysetNode request: [ "
              << request->ShortDebugString() << " ]";

    for (int i = 0; i < request->copysets_size(); ++i) {
        auto status = CreateOneCopyset(request->copysets(i));
        if (status != COPYSET_OP_STATUS_SUCCESS) {
            response->set_status(status);
            LOG(WARNING) << "Create copyset "
                         << ToGroupIdString(request->copysets(i).poolid(),
                                            request->copysets(i).copysetid())
                         << " failed";
            return;
        }
    }

    response->set_status(COPYSET_OP_STATUS_SUCCESS);
    LOG(INFO) << "CreateCopysetNode request: [ " << request->ShortDebugString()
              << " ] success";
}

void CopysetServiceImpl::GetCopysetStatus(
    google::protobuf::RpcController* controller,
    const CopysetStatusRequest* request, CopysetStatusResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    return GetOneCopysetStatus(*request, response);
}

void CopysetServiceImpl::GetCopysetsStatus(
    google::protobuf::RpcController* controller,
    const CopysetsStatusRequest* request, CopysetsStatusResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);

    for (int i = 0; i < request->copysets_size(); ++i) {
        GetOneCopysetStatus(request->copysets(i), response->add_status());
    }
}

COPYSET_OP_STATUS CopysetServiceImpl::CreateOneCopyset(
    const CreateCopysetRequest::Copyset& copyset) {
    bool exists =
        manager_->IsCopysetNodeExist(copyset.poolid(), copyset.copysetid());
    if (exists) {
        LOG(WARNING) << "Copyset "
                     << ToGroupIdString(copyset.poolid(), copyset.copysetid())
                     << " already exists";
        return COPYSET_OP_STATUS_EXIST;
    }

    braft::Configuration conf;
    for (int i = 0; i < copyset.peers_size(); ++i) {
        braft::PeerId peerId;
        int ret = peerId.parse(copyset.peers(i).address());
        if (ret != 0) {
            LOG(WARNING) << "Crate copyset "
                         << ToGroupIdString(copyset.poolid(),
                                            copyset.copysetid())
                         << " failed because parse peer from "
                         << copyset.peers(i).address() << " failed";
            return COPYSET_OP_STATUS_PARSE_PEER_ERROR;
        }

        conf.add_peer(braft::PeerId(copyset.peers(i).address()));
    }

    bool success = manager_->CreateCopysetNode(copyset.poolid(),
                                               copyset.copysetid(), conf);
    if (success) {
        LOG(INFO) << "Create copyset "
                  << ToGroupIdString(copyset.poolid(), copyset.copysetid())
                  << " success";
        return COPYSET_OP_STATUS_SUCCESS;
    } else {
        LOG(WARNING) << "Create copyset "
                     << ToGroupIdString(copyset.poolid(), copyset.copysetid())
                     << " failed";
        return COPYSET_OP_STATUS_FAILURE_UNKNOWN;
    }
}

void CopysetServiceImpl::GetOneCopysetStatus(
    const CopysetStatusRequest& request, CopysetStatusResponse* response) {
    auto* node =
        manager_->GetCopysetNode(request.poolid(), request.copysetid());

    if (!node) {
        LOG(WARNING) << "GetCopysetStauts failed, copyset "
                     << ToGroupIdString(request.poolid(), request.copysetid())
                     << " not exists";
        response->set_status(
            COPYSET_OP_STATUS::COPYSET_OP_STATUS_COPYSET_NOTEXIST);
        return;
    }

    braft::NodeStatus status;
    node->GetStatus(&status);

    auto* copysetStatus = response->mutable_copysetstatus();

    copysetStatus->set_state(status.state);
    copysetStatus->mutable_peer()->set_address(status.peer_id.to_string());
    copysetStatus->mutable_leader()->set_address(status.leader_id.to_string());
    copysetStatus->set_readonly(status.readonly);
    copysetStatus->set_term(status.term);
    copysetStatus->set_committedindex(status.committed_index);
    copysetStatus->set_knownappliedindex(status.known_applied_index);
    copysetStatus->set_pendingindex(status.pending_index);
    copysetStatus->set_pendingqueuesize(status.pending_queue_size);
    copysetStatus->set_applyingindex(status.applying_index);
    copysetStatus->set_firstindex(status.first_index);
    copysetStatus->set_lastindex(status.last_index);
    copysetStatus->set_diskindex(status.disk_index);

    copysetStatus->set_epoch(node->GetConfEpoch());

    if (request.queryhash()) {
        // TODO(wuhanqing): implement hash
    }

    response->set_status(COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS);
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

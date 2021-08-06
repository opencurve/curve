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
 * Date: Wed Sep  1 11:17:56 CST 2021
 * Author: wuhanqing
 */

// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#include "curvefs/src/metaserver/copyset/raft_cli_service2.h"

#include <braft/node_manager.h>

#include <string>
#include <vector>

#include "curvefs/src/metaserver/copyset/types.h"
#include "curvefs/src/metaserver/copyset/utils.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

void RaftCliService2::GetLeader(
    ::google::protobuf::RpcController* controller,
    const ::curvefs::metaserver::copyset::GetLeaderRequest2* request,
    ::curvefs::metaserver::copyset::GetLeaderResponse2* response,
    ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    std::vector<scoped_refptr<braft::NodeImpl>> nodes;
    braft::NodeManager* const nm = braft::NodeManager::GetInstance();
    GroupId groupId = ToGroupId(request->poolid(), request->copysetid());

    nm->get_nodes_by_group_id(groupId, &nodes);
    if (nodes.empty()) {
        cntl->SetFailed(ENOENT, "No nodes in group %s", groupId.c_str());
        return;
    }

    for (size_t i = 0; i < nodes.size(); ++i) {
        braft::PeerId leaderId = nodes[i]->leader_id();
        if (!leaderId.is_empty()) {
            response->mutable_leader()->set_address(leaderId.to_string());
            return;
        }
    }

    cntl->SetFailed(EAGAIN, "Unknown leader");
}

butil::Status RaftCliService2::GetNode(scoped_refptr<braft::NodeImpl>* node,
                                       PoolId poolId, CopysetId copysetId,
                                       const std::string& peerId) {
    GroupId groupId = ToGroupId(poolId, copysetId);
    braft::NodeManager* const nm = braft::NodeManager::GetInstance();

    *node = nm->get(groupId, peerId);
    if (!(*node)) {
        return butil::Status(ENOENT, "Fail to find node %s in group %s",
                             peerId.c_str(), groupId.c_str());
    }

    if ((*node)->disable_cli()) {
        return butil::Status(EACCES,
                             "CliService2 is not allowed to access node %s",
                             (*node)->node_id().to_string().c_str());
    }

    return butil::Status::OK();
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

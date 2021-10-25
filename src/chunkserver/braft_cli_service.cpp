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

#include "src/chunkserver/braft_cli_service.h"

#include <glog/logging.h>
#include <brpc/controller.h>             // brpc::Controller
#include <braft/node_manager.h>          // NodeManager
#include <braft/closure_helper.h>        // NewCallback

#include <cerrno>
#include <vector>
#include <string>

namespace curve {
namespace chunkserver {

static void add_peer_returned(brpc::Controller *cntl,
                              const AddPeerRequest *request,
                              AddPeerResponse *response,
                              std::vector<braft::PeerId> old_peers,
                              scoped_refptr<braft::NodeImpl> /*node*/,
                              Closure *done,
                              const butil::Status &st) {
    brpc::ClosureGuard done_guard(done);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    bool already_exists = false;
    for (size_t i = 0; i < old_peers.size(); ++i) {
        response->add_old_peers(old_peers[i].to_string());
        response->add_new_peers(old_peers[i].to_string());
        if (old_peers[i] == request->peer_id()) {
            already_exists = true;
        }
    }
    if (!already_exists) {
        response->add_new_peers(request->peer_id());
    }
}

void BRaftCliServiceImpl::add_peer(RpcController *controller,
                                   const AddPeerRequest *request,
                                   AddPeerResponse *response,
                                   Closure *done) {
    brpc::Controller *cntl = (brpc::Controller *) controller;
    brpc::ClosureGuard done_guard(done);
    scoped_refptr<braft::NodeImpl> node;
    LogicPoolID logicPoolId = request->logicpoolid();
    CopysetID copysetId = request->copysetid();
    butil::Status
        st = get_node(&node, logicPoolId, copysetId, request->leader_id());
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    std::vector<braft::PeerId> peers;
    st = node->list_peers(&peers);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    braft::PeerId adding_peer;
    if (adding_peer.parse(request->peer_id()) != 0) {
        cntl->SetFailed(EINVAL, "Fail to parse peer_id %s",
                        request->peer_id().c_str());
        return;
    }
    LOG(WARNING) << "Receive AddPeerRequest to " << node->node_id()
                 << " from " << cntl->remote_side()
                 << ", adding " << request->peer_id();
    braft::Closure *add_peer_done = NewCallback(
        add_peer_returned, cntl, request, response, peers, node,
        done_guard.release());
    return node->add_peer(adding_peer, add_peer_done);
}

static void remove_peer_returned(brpc::Controller *cntl,
                                 const RemovePeerRequest *request,
                                 RemovePeerResponse *response,
                                 std::vector<braft::PeerId> old_peers,
                                 scoped_refptr<braft::NodeImpl> /*node*/,
                                 Closure *done,
                                 const butil::Status &st) {
    brpc::ClosureGuard done_guard(done);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    for (size_t i = 0; i < old_peers.size(); ++i) {
        response->add_old_peers(old_peers[i].to_string());
        if (old_peers[i] != request->peer_id()) {
            response->add_new_peers(old_peers[i].to_string());
        }
    }
}

void BRaftCliServiceImpl::remove_peer(RpcController *controller,
                                      const RemovePeerRequest *request,
                                      RemovePeerResponse *response,
                                      Closure *done) {
    brpc::Controller *cntl = (brpc::Controller *) controller;
    brpc::ClosureGuard done_guard(done);
    scoped_refptr<braft::NodeImpl> node;
    LogicPoolID logicPoolId = request->logicpoolid();
    CopysetID copysetId = request->copysetid();
    butil::Status
        st = get_node(&node, logicPoolId, copysetId, request->leader_id());
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    std::vector<braft::PeerId> peers;
    st = node->list_peers(&peers);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    braft::PeerId removing_peer;
    if (removing_peer.parse(request->peer_id()) != 0) {
        cntl->SetFailed(EINVAL, "Fail to parse peer_id %s",
                        request->peer_id().c_str());
        return;
    }
    LOG(WARNING) << "Receive RemovePeerRequest to " << node->node_id()
                 << " from " << cntl->remote_side()
                 << ", removing " << request->peer_id();
    braft::Closure *remove_peer_done = NewCallback(
        remove_peer_returned, cntl, request, response, peers, node,
        done_guard.release());
    return node->remove_peer(removing_peer, remove_peer_done);
}

void BRaftCliServiceImpl::get_leader(RpcController *controller,
                                     const GetLeaderRequest *request,
                                     GetLeaderResponse *response,
                                     Closure *done) {
    brpc::Controller *cntl = (brpc::Controller *) controller;
    brpc::ClosureGuard done_guard(done);
    std::vector<scoped_refptr<braft::NodeImpl> > nodes;
    braft::NodeManager *const nm = braft::NodeManager::GetInstance();
    LogicPoolID logicPoolId = request->logicpoolid();
    CopysetID copysetId = request->copysetid();
    braft::GroupId group_id = ToGroupId(logicPoolId, copysetId);
    nm->get_nodes_by_group_id(group_id, &nodes);
    if (nodes.empty()) {
        cntl->SetFailed(ENOENT, "No nodes in group %s",
                        group_id.c_str());
        return;
    }
    for (size_t i = 0; i < nodes.size(); ++i) {
        braft::PeerId leader_id = nodes[i]->leader_id();
        if (!leader_id.is_empty()) {
            response->set_leader_id(leader_id.to_string());
            return;
        }
    }
    cntl->SetFailed(EAGAIN, "Unknown leader");
}

butil::Status BRaftCliServiceImpl::get_node(scoped_refptr<braft::NodeImpl> *node,   //NOLINT
                                            const LogicPoolID &logicPoolId,
                                            const CopysetID &copysetId,
                                            const std::string &peer_id) {
    braft::GroupId group_id = ToGroupId(logicPoolId, copysetId);
    braft::NodeManager *const nm = braft::NodeManager::GetInstance();
    /* peer id is required have been guaranteed in proto */
    *node = nm->get(group_id, peer_id);
    if (!(*node)) {
        return butil::Status(ENOENT, "Fail to find node %s in group %s",
                             peer_id.c_str(),
                             group_id.c_str());
    }

    if ((*node)->disable_cli()) {
        return butil::Status(EACCES,
                             "CliService is not allowed to access node "
                             "%s",
                             (*node)->node_id().to_string().c_str());
    }

    return butil::Status::OK();
}

void BRaftCliServiceImpl::transfer_leader(
    RpcController *controller,
    const TransferLeaderRequest *request,
    TransferLeaderResponse *response,
    ::google::protobuf::Closure *done) {
    brpc::Controller *cntl = (brpc::Controller *) controller;
    brpc::ClosureGuard done_guard(done);
    scoped_refptr<braft::NodeImpl> node;
    LogicPoolID logicPoolId = request->logicpoolid();
    CopysetID copysetId = request->copysetid();
    butil::Status
        st = get_node(&node, logicPoolId, copysetId, request->leader_id());
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    braft::PeerId peer = braft::ANY_PEER;
    if (request->has_peer_id() && peer.parse(request->peer_id()) != 0) {
        cntl->SetFailed(EINVAL, "Fail to parse %s", request->peer_id().c_str());
        return;
    }
    const int rc = node->transfer_leadership_to(peer);
    if (rc != 0) {
        cntl->SetFailed(rc, "Fail to invoke transfer_leadership_to : %s",
                        berror(rc));
        return;
    }
}

}  // namespace chunkserver
}  // namespace curve

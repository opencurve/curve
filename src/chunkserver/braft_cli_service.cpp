/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/braft_cli_service.h"

#include <brpc/controller.h>             // brpc::Controller
#include <braft/node_manager.h>          // NodeManager
#include <braft/closure_helper.h>        // NewCallback

#include <cerrno>
#include <vector>
#include <string>

namespace curve {
namespace chunkserver {

static void add_peer_returned(brpc::Controller *cntl,
                              const ::curve::chunkserver::AddPeerRequest *request,
                              ::curve::chunkserver::AddPeerResponse *response,
                              std::vector<braft::PeerId> old_peers,
                              scoped_refptr<braft::NodeImpl> /*node*/,
                              ::google::protobuf::Closure *done,
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

void BRaftCliServiceImpl::add_peer(::google::protobuf::RpcController *controller,
                                   const ::curve::chunkserver::AddPeerRequest *request,
                                   ::curve::chunkserver::AddPeerResponse *response,
                                   ::google::protobuf::Closure *done) {
    brpc::Controller *cntl = (brpc::Controller *) controller;
    brpc::ClosureGuard done_guard(done);
    scoped_refptr<braft::NodeImpl> node;
    LogicPoolID logicPoolId = request->logicpoolid();
    CopysetID copysetId = request->copysetid();
    butil::Status st = get_node(&node, logicPoolId, copysetId, request->leader_id());
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
                                 const ::curve::chunkserver::RemovePeerRequest *request,
                                 ::curve::chunkserver::RemovePeerResponse *response,
                                 std::vector<braft::PeerId> old_peers,
                                 scoped_refptr<braft::NodeImpl> /*node*/,
                                 ::google::protobuf::Closure *done,
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

void BRaftCliServiceImpl::remove_peer(::google::protobuf::RpcController *controller,
                                      const ::curve::chunkserver::RemovePeerRequest *request,
                                      ::curve::chunkserver::RemovePeerResponse *response,
                                      ::google::protobuf::Closure *done) {
    brpc::Controller *cntl = (brpc::Controller *) controller;
    brpc::ClosureGuard done_guard(done);
    scoped_refptr<braft::NodeImpl> node;
    LogicPoolID logicPoolId = request->logicpoolid();
    CopysetID copysetId = request->copysetid();
    butil::Status st = get_node(&node, logicPoolId, copysetId, request->leader_id());
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

void BRaftCliServiceImpl::reset_peer(::google::protobuf::RpcController *controller,
                                     const ::curve::chunkserver::ResetPeerRequest *request,
                                     ::curve::chunkserver::ResetPeerResponse *response,
                                     ::google::protobuf::Closure *done) {
    brpc::Controller *cntl = (brpc::Controller *) controller;
    brpc::ClosureGuard done_guard(done);
    scoped_refptr<braft::NodeImpl> node;
    LogicPoolID logicPoolId = request->logicpoolid();
    CopysetID copysetId = request->copysetid();
    butil::Status st = get_node(&node, logicPoolId, copysetId, request->peer_id());
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    braft::Configuration new_peers;
    for (int i = 0; i < request->new_peers_size(); ++i) {
        braft::PeerId peer;
        if (peer.parse(request->new_peers(i)) != 0) {
            cntl->SetFailed(EINVAL, "Fail to parse %s",
                            request->new_peers(i).c_str());
            return;
        }
        new_peers.add_peer(peer);
    }
    LOG(WARNING) << "Receive set_peer to " << node->node_id()
                 << " from " << cntl->remote_side();
    st = node->reset_peers(new_peers);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    }
}

static void snapshot_returned(brpc::Controller *cntl,
                              scoped_refptr<braft::NodeImpl> node,
                              ::google::protobuf::Closure *done,
                              const butil::Status &st) {
    brpc::ClosureGuard done_guard(done);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
    }
}

void BRaftCliServiceImpl::snapshot(::google::protobuf::RpcController *controller,
                                   const ::curve::chunkserver::SnapshotRequest *request,
                                   ::curve::chunkserver::SnapshotResponse *response,
                                   ::google::protobuf::Closure *done) {
    brpc::Controller *cntl = (brpc::Controller *) controller;
    brpc::ClosureGuard done_guard(done);
    scoped_refptr<braft::NodeImpl> node;
    LogicPoolID logicPoolId = request->logicpoolid();
    CopysetID copysetId = request->copysetid();
    butil::Status st = get_node(&node, logicPoolId, copysetId, request->peer_id());
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    braft::Closure *snapshot_done = NewCallback(snapshot_returned, cntl, node,
                                                done_guard.release());
    return node->snapshot(snapshot_done);
}

void BRaftCliServiceImpl::get_leader(::google::protobuf::RpcController *controller,
                                     const ::curve::chunkserver::GetLeaderRequest *request,
                                     ::curve::chunkserver::GetLeaderResponse *response,
                                     ::google::protobuf::Closure *done) {
    brpc::Controller *cntl = (brpc::Controller *) controller;
    brpc::ClosureGuard done_guard(done);
    std::vector<scoped_refptr<braft::NodeImpl> > nodes;
    braft::NodeManager *const nm = braft::NodeManager::GetInstance();
    LogicPoolID logicPoolId = request->logicpoolid();
    CopysetID copysetId = request->copysetid();
    braft::GroupId group_id = ToGroupId(logicPoolId, copysetId);
    if (request->has_peer_id()) {
        braft::PeerId peer;
        if (peer.parse(request->peer_id()) != 0) {
            cntl->SetFailed(EINVAL, "Fail to parse %s",
                            request->peer_id().c_str());
            return;
        }
        scoped_refptr<braft::NodeImpl> node = nm->get(group_id, peer);
        if (node) {
            nodes.push_back(node);
        }
    } else {
        nm->get_nodes_by_group_id(group_id, &nodes);
    }
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

butil::Status BRaftCliServiceImpl::get_node(scoped_refptr<braft::NodeImpl> *node,
                                            const LogicPoolID &logicPoolId,
                                            const CopysetID &copysetId,
                                            const std::string &peer_id) {
    braft::GroupId group_id = ToGroupId(logicPoolId, copysetId);
    braft::NodeManager *const nm = braft::NodeManager::GetInstance();
    if (!peer_id.empty()) {
        *node = nm->get(group_id, peer_id);
        if (!(*node)) {
            return butil::Status(ENOENT, "Fail to find node %s in group %s",
                                 peer_id.c_str(),
                                 group_id.c_str());
        }
    } else {
        std::vector<scoped_refptr<braft::NodeImpl> > nodes;
        nm->get_nodes_by_group_id(group_id, &nodes);
        if (nodes.empty()) {
            return butil::Status(ENOENT, "Fail to find node in group %s",
                                 group_id.c_str());
        }
        if (nodes.size() > 1) {
            return butil::Status(EINVAL, "peer must be specified "
                                         "since there're %lu nodes in group %s",
                                 nodes.size(), group_id.c_str());
        }
        *node = nodes.front();
    }

    if ((*node)->disable_cli()) {
        return butil::Status(EACCES, "CliService is not allowed to access node "
                                     "%s", (*node)->node_id().to_string().c_str());
    }

    return butil::Status::OK();
}

static void change_peers_returned(brpc::Controller *cntl,
                                  const ::curve::chunkserver::ChangePeersRequest *request,
                                  ::curve::chunkserver::ChangePeersResponse *response,
                                  std::vector<braft::PeerId> old_peers,
                                  braft::Configuration new_peers,
                                  scoped_refptr<braft::NodeImpl> /*node*/,
                                  ::google::protobuf::Closure *done,
                                  const butil::Status &st) {
    brpc::ClosureGuard done_guard(done);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    for (size_t i = 0; i < old_peers.size(); ++i) {
        response->add_old_peers(old_peers[i].to_string());
    }
    for (braft::Configuration::const_iterator
             iter = new_peers.begin(); iter != new_peers.end(); ++iter) {
        response->add_new_peers(iter->to_string());
    }
}

void BRaftCliServiceImpl::change_peers(::google::protobuf::RpcController *controller,
                                       const ::curve::chunkserver::ChangePeersRequest *request,
                                       ::curve::chunkserver::ChangePeersResponse *response,
                                       ::google::protobuf::Closure *done) {
    brpc::Controller *cntl = (brpc::Controller *) controller;
    brpc::ClosureGuard done_guard(done);
    scoped_refptr<braft::NodeImpl> node;
    LogicPoolID logicPoolId = request->logicpoolid();
    CopysetID copysetId = request->copysetid();
    butil::Status st = get_node(&node, logicPoolId, copysetId, request->leader_id());
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    std::vector<braft::PeerId> old_peers;
    st = node->list_peers(&old_peers);
    if (!st.ok()) {
        cntl->SetFailed(st.error_code(), "%s", st.error_cstr());
        return;
    }
    braft::Configuration conf;
    for (int i = 0; i < request->new_peers_size(); ++i) {
        braft::PeerId peer;
        if (peer.parse(request->new_peers(i)) != 0) {
            cntl->SetFailed(EINVAL, "Fail to parse %s",
                            request->new_peers(i).c_str());
            return;
        }
        conf.add_peer(peer);
    }
    braft::Closure *change_peers_done = NewCallback(
        change_peers_returned,
        cntl, request, response, old_peers, conf, node,
        done_guard.release());
    return node->change_peers(conf, change_peers_done);
}

void BRaftCliServiceImpl::transfer_leader(
    ::google::protobuf::RpcController *controller,
    const ::curve::chunkserver::TransferLeaderRequest *request,
    ::curve::chunkserver::TransferLeaderResponse *response,
    ::google::protobuf::Closure *done) {
    brpc::Controller *cntl = (brpc::Controller *) controller;
    brpc::ClosureGuard done_guard(done);
    scoped_refptr<braft::NodeImpl> node;
    LogicPoolID logicPoolId = request->logicpoolid();
    CopysetID copysetId = request->copysetid();
    butil::Status st = get_node(&node, logicPoolId, copysetId, request->leader_id());
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

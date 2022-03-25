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

#include <braft/closure_helper.h>
#include <braft/node_manager.h>
#include <butil/errno.h>
#include <google/protobuf/util/message_differencer.h>

#include <string>
#include <utility>
#include <vector>

#include "curvefs/src/metaserver/copyset/types.h"
#include "curvefs/src/metaserver/copyset/utils.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

namespace {

using ::google::protobuf::util::MessageDifferencer;

struct OnAddPeerReturned : public braft::Closure {
    OnAddPeerReturned(brpc::Controller* cntl, const AddPeerRequest2* request,
                      AddPeerResponse2* response, std::vector<Peer>&& oldPeers,
                      google::protobuf::Closure* done)
        : cntl(cntl),
          request(request),
          response(response),
          oldPeers(std::move(oldPeers)),
          done(done) {}

    void Run() {
        brpc::ClosureGuard doneGuard(done);
        if (!status().ok()) {
            cntl->SetFailed(status().error_code(), "%s", status().error_cstr());
            return;
        }

        *response->mutable_oldpeers() = {oldPeers.begin(), oldPeers.end()};
        *response->mutable_newpeers() = {oldPeers.begin(), oldPeers.end()};

        auto iter = std::find_if(
            oldPeers.begin(), oldPeers.end(), [this](const Peer& p) {
                return MessageDifferencer::Equals(request->addpeer(), p);
            });

        if (iter == oldPeers.end()) {
            *response->add_newpeers() = request->addpeer();
        }
    }

    brpc::Controller* cntl;
    const AddPeerRequest2* request;
    AddPeerResponse2* response;
    std::vector<Peer> oldPeers;
    google::protobuf::Closure* done;
};

struct OnRemovePeerReturned : public braft::Closure {
    OnRemovePeerReturned(brpc::Controller* cntl,
                         const RemovePeerRequest2* request,
                         RemovePeerResponse2* response,
                         std::vector<Peer>&& oldPeers,
                         google::protobuf::Closure* done)
        : cntl(cntl),
          request(request),
          response(response),
          oldPeers(std::move(oldPeers)),
          done(done) {}

    void Run() override {
        brpc::ClosureGuard doneGuard(done);
        if (!status().ok()) {
            cntl->SetFailed(status().error_code(), "%s", status().error_cstr());
            return;
        }

        for (size_t i = 0; i < oldPeers.size(); ++i) {
            *response->add_oldpeers() = oldPeers[i];
            if (!MessageDifferencer::Equals(oldPeers[i],
                                            request->removepeer())) {
                *response->add_newpeers() = oldPeers[i];
            }
        }
    }

    brpc::Controller* cntl;
    const RemovePeerRequest2* request;
    RemovePeerResponse2* response;
    std::vector<Peer> oldPeers;
    google::protobuf::Closure* done;
};

struct OnChangePeersReturned : public braft::Closure {
    OnChangePeersReturned(brpc::Controller* cntl,
                          const ChangePeersRequest2* request,
                          ChangePeersResponse2* response,
                          std::vector<Peer>&& oldPeers,
                          const std::vector<Peer>& newPeers,
                          google::protobuf::Closure* done)
        : cntl(cntl),
          request(request),
          response(response),
          oldPeers(std::move(oldPeers)),
          newPeers(newPeers),
          done(done) {}

    void Run() override {
        brpc::ClosureGuard doneGuard(done);
        if (!status().ok()) {
            cntl->SetFailed(status().error_code(), "%s", status().error_cstr());
            return;
        }

        *response->mutable_oldpeers() = {oldPeers.begin(), oldPeers.end()};
        *response->mutable_newpeers() = {newPeers.begin(), newPeers.end()};
    }

    brpc::Controller* cntl;
    const ChangePeersRequest2* request;
    ChangePeersResponse2* response;
    std::vector<Peer> oldPeers;
    std::vector<Peer> newPeers;
    google::protobuf::Closure* done;
};

}  // namespace

RaftCliService2::RaftCliService2(CopysetNodeManager* nodeManager)
    : nodeManager_(nodeManager) {}

void RaftCliService2::GetLeader(google::protobuf::RpcController* controller,
                                const GetLeaderRequest2* request,
                                GetLeaderResponse2* response,
                                google::protobuf::Closure* done) {
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

void RaftCliService2::AddPeer(google::protobuf::RpcController* controller,
                              const AddPeerRequest2* request,
                              AddPeerResponse2* response,
                              google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    auto* node =
        nodeManager_->GetCopysetNode(request->poolid(), request->copysetid());
    if (!node) {
        cntl->SetFailed(
            ENOENT, "Copyset %s not found",
            ToGroupIdString(request->poolid(), request->copysetid()).c_str());
        return;
    }

    std::vector<Peer> oldPeers;
    node->ListPeers(&oldPeers);

    auto* addPeerDone = new OnAddPeerReturned(
        cntl, request, response, std::move(oldPeers), doneGuard.release());
    return node->AddPeer(request->addpeer(), addPeerDone);
}

void RaftCliService2::RemovePeer(google::protobuf::RpcController* controller,
                                 const RemovePeerRequest2* request,
                                 RemovePeerResponse2* response,
                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    auto* node =
        nodeManager_->GetCopysetNode(request->poolid(), request->copysetid());
    if (!node) {
        cntl->SetFailed(
            ENOENT, "Copyset %s not found",
            ToGroupIdString(request->poolid(), request->copysetid()).c_str());
        return;
    }

    std::vector<Peer> oldPeers;
    node->ListPeers(&oldPeers);

    auto* removePeerDone = new OnRemovePeerReturned(
        cntl, request, response, std::move(oldPeers), doneGuard.release());
    return node->RemovePeer(request->removepeer(), removePeerDone);
}

void RaftCliService2::ChangePeers(google::protobuf::RpcController* controller,
                                  const ChangePeersRequest2* request,
                                  ChangePeersResponse2* response,
                                  google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    auto* node =
        nodeManager_->GetCopysetNode(request->poolid(), request->copysetid());
    if (!node) {
        cntl->SetFailed(
            ENOENT, "Copyset %s not found",
            ToGroupIdString(request->poolid(), request->copysetid()).c_str());
        return;
    }

    std::vector<Peer> oldPeers;
    node->ListPeers(&oldPeers);

    std::vector<Peer> newPeers{request->newpeers().begin(),
                               request->newpeers().end()};

    auto* changePeerDone =
        new OnChangePeersReturned(cntl, request, response, std::move(oldPeers),
                                  newPeers, doneGuard.release());
    return node->ChangePeers(newPeers, changePeerDone);
}

void RaftCliService2::TransferLeader(
    google::protobuf::RpcController* controller,
    const TransferLeaderRequest2* request,
    TransferLeaderResponse2* /* response */, google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    auto* node =
        nodeManager_->GetCopysetNode(request->poolid(), request->copysetid());
    if (!node) {
        cntl->SetFailed(
            ENOENT, "Copyset %s not found",
            ToGroupIdString(request->poolid(), request->copysetid()).c_str());
        return;
    }

    auto st = node->TransferLeader(request->transferee());
    if (!st.ok()) {
        cntl->SetFailed(
            st.error_code(),
            "Fail to TransferLeader to %s, copyset %s, error: %s",
            request->transferee().ShortDebugString().c_str(),
            ToGroupIdString(request->poolid(), request->copysetid()).c_str(),
            st.error_cstr());
    }
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

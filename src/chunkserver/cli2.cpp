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
 * Created Date: 18-8-27
 * Author: wudemiao
 */

#include "src/chunkserver/cli2.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <butil/status.h>
#include <glog/logging.h>

#include <string>

#include "proto/cli2.pb.h"

namespace curve {
namespace chunkserver {

butil::Status GetLeader(const LogicPoolID& logicPoolId,
                        const CopysetID& copysetId, const Configuration& conf,
                        Peer* leader) {
    if (conf.empty()) {
        return butil::Status(EINVAL, "Empty group configuration");
    }

    butil::Status st(-1, "Fail to get leader of copyset node %s",
                     ToGroupIdString(logicPoolId, copysetId).c_str());
    PeerId leaderId;
    Configuration::const_iterator iter = conf.begin();
    for (; iter != conf.end(); ++iter) {
        brpc::Channel channel;
        if (channel.Init(iter->addr, NULL) != 0) {
            return butil::Status(-1, "Fail to init channel to %s",
                                 iter->to_string().c_str());
        }
        Peer* peer = new Peer();
        CliService2_Stub stub(&channel);
        GetLeaderRequest2 request;
        GetLeaderResponse2 response;
        brpc::Controller cntl;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_allocated_peer(peer);
        peer->set_address(iter->to_string());

        stub.GetLeader(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            std::string saved_et = st.error_str();
            st.set_error(cntl.ErrorCode(), "%s, [%s] %s", saved_et.c_str(),
                         butil::endpoint2str(cntl.remote_side()).c_str(),
                         cntl.ErrorText().c_str());
            continue;
        } else {
            *leader = response.leader();
            leaderId.parse(leader->address());
            break;
        }
    }
    if (leaderId.is_empty()) {
        return st;
    }
    LOG(INFO) << "Get leader from " << iter->to_string().c_str()
              << " success, leader is " << leaderId;
    return butil::Status::OK();
}

butil::Status AddPeer(const LogicPoolID& logicPoolId,
                      const CopysetID& copysetId, const Configuration& conf,
                      const Peer& peer, const braft::cli::CliOptions& options) {
    Peer leader;
    butil::Status st = GetLeader(logicPoolId, copysetId, conf, &leader);
    BRAFT_RETURN_IF(!st.ok(), st);
    brpc::Channel channel;
    PeerId leaderId(leader.address());
    if (channel.Init(leaderId.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                             leaderId.to_string().c_str());
    }
    AddPeerRequest2 request;
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    Peer* leaderPeer = new Peer();
    request.set_allocated_leader(leaderPeer);
    *leaderPeer = leader;
    Peer* addPeer = new Peer();
    request.set_allocated_addpeer(addPeer);
    *addPeer = peer;
    AddPeerResponse2 response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService2_Stub stub(&channel);
    stub.AddPeer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.oldpeers().size(); ++i) {
        PeerId peer(response.oldpeers(i).address());
        old_conf.add_peer(peer);
    }
    Configuration new_conf;
    for (int i = 0; i < response.newpeers().size(); ++i) {
        PeerId peer(response.newpeers(i).address());
        new_conf.add_peer(peer);
    }
    LOG(INFO) << "Configuration of replication group ` "
              << ToGroupIdString(logicPoolId, copysetId) << " ' changed from "
              << old_conf << " to " << new_conf;
    return butil::Status::OK();
}

butil::Status RemovePeer(const LogicPoolID& logicPoolId,
                         const CopysetID& copysetId, const Configuration& conf,
                         const Peer& peer,
                         const braft::cli::CliOptions& options) {
    Peer leader;
    butil::Status st = GetLeader(logicPoolId, copysetId, conf, &leader);
    BRAFT_RETURN_IF(!st.ok(), st);
    PeerId leaderId(leader.address());
    brpc::Channel channel;
    if (channel.Init(leaderId.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                             leaderId.to_string().c_str());
    }
    RemovePeerRequest2 request;
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    Peer* leaderPeer = new Peer();
    request.set_allocated_leader(leaderPeer);
    *leaderPeer = leader;
    Peer* removePeer = new Peer();
    request.set_allocated_removepeer(removePeer);
    *removePeer = peer;
    RemovePeerResponse2 response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService2_Stub stub(&channel);
    stub.RemovePeer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(INFO) << "remove peer failed: " << cntl.ErrorText();
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.oldpeers().size(); ++i) {
        PeerId peer(response.oldpeers(i).address());
        old_conf.add_peer(peer);
    }
    Configuration new_conf;
    for (int i = 0; i < response.newpeers().size(); ++i) {
        PeerId peer(response.newpeers(i).address());
        new_conf.add_peer(peer);
    }
    LOG(INFO) << "Configuration of replication group ` "
              << ToGroupIdString(logicPoolId, copysetId) << " ' changed from "
              << old_conf << " to " << new_conf;
    return butil::Status::OK();
}

butil::Status ChangePeers(const LogicPoolID& logicPoolId,
                          const CopysetID& copysetId, const Configuration& conf,
                          const Configuration& newPeers,
                          const braft::cli::CliOptions& options) {
    Peer leader;
    butil::Status st = GetLeader(logicPoolId, copysetId, conf, &leader);
    BRAFT_RETURN_IF(!st.ok(), st);
    PeerId leaderId(leader.address());
    brpc::Channel channel;
    if (channel.Init(leaderId.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                             leaderId.to_string().c_str());
    }

    ChangePeersRequest2 request;
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    Peer* leaderPeer = new Peer();
    *leaderPeer = leader;
    request.set_allocated_leader(leaderPeer);
    for (Configuration::const_iterator iter = newPeers.begin();
         iter != newPeers.end(); ++iter) {
        request.add_newpeers()->set_address(iter->to_string());
    }
    ChangePeersResponse2 response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService2_Stub stub(&channel);
    stub.ChangePeers(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.oldpeers_size(); ++i) {
        old_conf.add_peer(response.oldpeers(i).address());
    }
    Configuration new_conf;
    for (int i = 0; i < response.newpeers_size(); ++i) {
        new_conf.add_peer(response.newpeers(i).address());
    }
    LOG(INFO) << "Configuration of replication group `"
              << ToGroupIdString(logicPoolId, copysetId) << "' changed from "
              << old_conf << " to " << new_conf;
    return butil::Status::OK();
}

butil::Status TransferLeader(const LogicPoolID& logicPoolId,
                             const CopysetID& copysetId,
                             const Configuration& conf, const Peer& peer,
                             const braft::cli::CliOptions& options) {
    Peer leader;
    butil::Status st = GetLeader(logicPoolId, copysetId, conf, &leader);
    BRAFT_RETURN_IF(!st.ok(), st);
    if (leader.address() == peer.address()) {
        LOG(INFO) << "peer " << peer.address() << " is already the leader";
        return butil::Status::OK();
    }
    PeerId leaderId(leader.address());
    brpc::Channel channel;
    if (channel.Init(leaderId.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                             leaderId.to_string().c_str());
    }
    TransferLeaderRequest2 request;
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    Peer* leaderPeer = new Peer();
    request.set_allocated_leader(leaderPeer);
    *leaderPeer = leader;
    Peer* transfereePeer = new Peer();
    request.set_allocated_transferee(transfereePeer);
    *transfereePeer = peer;
    TransferLeaderResponse2 response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    CliService2_Stub stub(&channel);
    stub.TransferLeader(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

// reset peer does not follow a consistency protocol and directly resets them,
// thus posing certain risks Application scenario: Extreme situation where most
// nodes fail. In this case, the copyset will not be able to be written directly
// After half an hour, MDS will migrate the copyset on the suspended replica,
// which will be unavailable for a period of time. To cope with this scenario,
// we have introduced The reset peer tool directly resets replication group
// members to only contain surviving replicas.
// Precautions:
// 1. Before resetting the peer, it is necessary to confirm through the
//    check-copyset tool that most of the replicas in the replication group have
//    indeed been suspended.
// 2. When resetting the peer, ensure that the remaining replicas have the
//    latest data, otherwise there is a risk of data loss.
// 3. Reset peer is suitable for situations where the other two replicas cannot
//    be restored, otherwise it may disrupt the cluster.
butil::Status ResetPeer(const LogicPoolID& logicPoolId,
                        const CopysetID& copysetId,
                        const Configuration& newPeers, const Peer& requestPeer,
                        const braft::cli::CliOptions& options) {
    if (newPeers.empty()) {
        return butil::Status(EINVAL, "new_conf is empty");
    }
    PeerId requestPeerId(requestPeer.address());
    brpc::Channel channel;
    if (channel.Init(requestPeerId.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                             requestPeerId.to_string().c_str());
    }
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    ResetPeerRequest2 request;
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    Peer* requestPeerPtr = new Peer();
    *requestPeerPtr = requestPeer;
    request.set_allocated_requestpeer(requestPeerPtr);
    for (Configuration::const_iterator iter = newPeers.begin();
         iter != newPeers.end(); ++iter) {
        request.add_newpeers()->set_address(iter->to_string());
    }
    ResetPeerResponse2 response;
    CliService2_Stub stub(&channel);
    stub.ResetPeer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

butil::Status Snapshot(const LogicPoolID& logicPoolId,
                       const CopysetID& copysetId, const Peer& peer,
                       const braft::cli::CliOptions& options) {
    brpc::Channel channel;
    PeerId peerId(peer.address());
    if (channel.Init(peerId.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                             peerId.to_string().c_str());
    }
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    SnapshotRequest2 request;
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    Peer* peerPtr = new Peer(peer);
    request.set_allocated_peer(peerPtr);
    SnapshotResponse2 response;
    CliService2_Stub stub(&channel);
    stub.Snapshot(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

butil::Status SnapshotAll(const Peer& peer,
                          const braft::cli::CliOptions& options) {
    brpc::Channel channel;
    PeerId peerId(peer.address());
    if (channel.Init(peerId.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                             peerId.to_string().c_str());
    }
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    SnapshotAllRequest request;
    SnapshotAllResponse response;
    CliService2_Stub stub(&channel);
    stub.SnapshotAll(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

}  // namespace chunkserver
}  // namespace curve

/*
 * Project: curve
 * Created Date: 18-8-27
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/cli2.h"

#include <glog/logging.h>
#include <butil/status.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <string>

#include "proto/cli2.pb.h"

namespace curve {
namespace chunkserver {

butil::Status GetLeader(const LogicPoolID &logicPoolId,
                        const CopysetID &copysetId,
                        const Configuration &conf,
                        Peer *leader) {
    if (conf.empty()) {
        return butil::Status(EINVAL, "Empty group configuration");
    }

    butil::Status st(-1,
                     "Fail to get leader of copyset node %s",
                     ToGroupIdString(logicPoolId, copysetId).c_str());
    PeerId leaderId;
    for (Configuration::const_iterator
             iter = conf.begin(); iter != conf.end(); ++iter) {
        brpc::Channel channel;
        if (channel.Init(iter->addr, NULL) != 0) {
            return butil::Status(-1, "Fail to init channel to %s",
                                 iter->to_string().c_str());
        }
        Peer *peer = new Peer();
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
    return butil::Status::OK();
}

butil::Status AddPeer(const LogicPoolID &logicPoolId,
                      const CopysetID &copysetId,
                      const Configuration &conf,
                      const Peer &peer,
                      const braft::cli::CliOptions &options) {
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
    Peer *leaderPeer = new Peer();
    request.set_allocated_leader(leaderPeer);
    *leaderPeer = leader;
    Peer *addPeer = new Peer();
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
              << ToGroupIdString(logicPoolId, copysetId)
              << " ' changed from " << old_conf
              << " to " << new_conf;
    return butil::Status::OK();
}

butil::Status RemovePeer(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId,
                         const Configuration &conf,
                         const Peer &peer,
                         const braft::cli::CliOptions &options) {
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
    Peer *leaderPeer = new Peer();
    request.set_allocated_leader(leaderPeer);
    *leaderPeer = leader;
    Peer *removePeer = new Peer();
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
              << ToGroupIdString(logicPoolId, copysetId)
              << " ' changed from " << old_conf
              << " to " << new_conf;
    return butil::Status::OK();
}

butil::Status TransferLeader(const LogicPoolID &logicPoolId,
                             const CopysetID &copysetId,
                             const Configuration &conf,
                             const Peer &peer,
                             const braft::cli::CliOptions &options) {
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
    Peer *leaderPeer = new Peer();
    request.set_allocated_leader(leaderPeer);
    *leaderPeer = leader;
    Peer *transfereePeer = new Peer();
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

}  // namespace chunkserver
}  // namespace curve

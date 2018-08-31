/*
 * Project: curve
 * Created Date: 18-8-27
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/cli.h"

#include <butil/status.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <string>

#include "proto/cli.pb.h"

namespace curve {
namespace chunkserver {

butil::Status GetLeader(const LogicPoolID &logicPoolId, const CopysetID &copysetId, const Configuration &conf,
                        PeerId *leaderId) {
    if (conf.empty()) {
        return butil::Status(EINVAL, "Empty group configuration");
    }
    // Construct a brpc naming service to access all the nodes in this group
    butil::Status st(-1, "Fail to get leader of copyset node %s", ToGroupIdString(logicPoolId, copysetId).c_str());
    leaderId->reset();
    for (Configuration::const_iterator
             iter = conf.begin(); iter != conf.end(); ++iter) {
        brpc::Channel channel;
        if (channel.Init(iter->addr, NULL) != 0) {
            return butil::Status(-1, "Fail to init channel to %s",
                                 iter->to_string().c_str());
        }
        CliService_Stub stub(&channel);
        GetLeaderRequest request;
        GetLeaderResponse response;
        brpc::Controller cntl;
        request.set_logicpoolid(logicPoolId);
        request.set_copysetid(copysetId);
        request.set_peer_id(iter->to_string());
        stub.get_leader(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            if (st.ok()) {
                st.set_error(cntl.ErrorCode(), "[%s] %s",
                             butil::endpoint2str(cntl.remote_side()).c_str(),
                             cntl.ErrorText().c_str());
            } else {
                std::string saved_et = st.error_str();
                st.set_error(cntl.ErrorCode(), "%s, [%s] %s", saved_et.c_str(),
                             butil::endpoint2str(cntl.remote_side()).c_str(),
                             cntl.ErrorText().c_str());
            }
            continue;
        }
        leaderId->parse(response.leader_id());
    }
    if (leaderId->is_empty()) {
        return st;
    }
    return butil::Status::OK();
}

butil::Status AddPeer(const LogicPoolID &logicPoolId, const CopysetID &copysetId, const Configuration &conf,
                      const PeerId &peer_id, const braft::cli::CliOptions &options) {
    PeerId leaderId;
    butil::Status st = GetLeader(logicPoolId, copysetId, conf, &leaderId);
    BRAFT_RETURN_IF(!st.ok(), st);
    brpc::Channel channel;
    if (channel.Init(leaderId.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                             leaderId.to_string().c_str());
    }
    AddPeerRequest request;
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_leader_id(leaderId.to_string());
    request.set_peer_id(peer_id.to_string());
    AddPeerResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService_Stub stub(&channel);
    stub.add_peer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.old_peers_size(); ++i) {
        old_conf.add_peer(response.old_peers(i));
    }
    Configuration new_conf;
    for (int i = 0; i < response.new_peers_size(); ++i) {
        new_conf.add_peer(response.new_peers(i));
    }
    LOG(INFO) << "Configuration of replication group ` " << ToGroupIdString(logicPoolId, copysetId)
              << " ' changed from " << old_conf
              << " to " << new_conf;
    return butil::Status::OK();
}

butil::Status RemovePeer(const LogicPoolID &logicPoolId, const CopysetID &copysetId, const Configuration &conf,
                         const PeerId &peer_id, const braft::cli::CliOptions &options) {
    PeerId leaderId;
    butil::Status st = GetLeader(logicPoolId, copysetId, conf, &leaderId);
    BRAFT_RETURN_IF(!st.ok(), st);
    brpc::Channel channel;
    if (channel.Init(leaderId.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                             leaderId.to_string().c_str());
    }
    RemovePeerRequest request;
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_leader_id(leaderId.to_string());
    request.set_peer_id(peer_id.to_string());
    RemovePeerResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService_Stub stub(&channel);
    stub.remove_peer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.old_peers_size(); ++i) {
        old_conf.add_peer(response.old_peers(i));
    }
    Configuration new_conf;
    for (int i = 0; i < response.new_peers_size(); ++i) {
        new_conf.add_peer(response.new_peers(i));
    }
    LOG(INFO) << "Configuration of replication group ` " << ToGroupIdString(logicPoolId, copysetId)
              << " ' changed from " << old_conf
              << " to " << new_conf;
    return butil::Status::OK();
}

butil::Status TransferLeader(const LogicPoolID &logicPoolId, const CopysetID &copysetId, const Configuration &conf,
                             const PeerId &peer, const braft::cli::CliOptions &options) {
    PeerId leaderId;
    butil::Status st = GetLeader(logicPoolId, copysetId, conf, &leaderId);
    BRAFT_RETURN_IF(!st.ok(), st);
    if (leaderId == peer) {
        LOG(INFO) << "peer " << peer << " is already the leader";
        return butil::Status::OK();
    }
    brpc::Channel channel;
    if (channel.Init(leaderId.addr, NULL) != 0) {
        return butil::Status(-1, "Fail to init channel to %s",
                             leaderId.to_string().c_str());
    }
    TransferLeaderRequest request;
    request.set_logicpoolid(logicPoolId);
    request.set_copysetid(copysetId);
    request.set_leader_id(leaderId.to_string());
    if (!peer.is_empty()) {
        request.set_peer_id(peer.to_string());
    }
    TransferLeaderResponse response;
    brpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    CliService_Stub stub(&channel);
    stub.transfer_leader(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return butil::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return butil::Status::OK();
}

}  // namespace chunkserver
}  // namespace curve

/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_CLI_SERVICE_H
#define CURVE_CHUNKSERVER_CLI_SERVICE_H

#include <butil/status.h>
#include <braft/node.h>

#include <string>

#include "src/chunkserver/copyset_node.h"
#include "proto/cli.pb.h"

namespace curve {
namespace chunkserver {

class BRaftCliServiceImpl : public CliService {
 public:
    void add_peer(::google::protobuf::RpcController *controller,
                  const ::curve::chunkserver::AddPeerRequest *request,
                  ::curve::chunkserver::AddPeerResponse *response,
                  ::google::protobuf::Closure *done);
    void remove_peer(::google::protobuf::RpcController *controller,
                     const ::curve::chunkserver::RemovePeerRequest *request,
                     ::curve::chunkserver::RemovePeerResponse *response,
                     ::google::protobuf::Closure *done);
    void reset_peer(::google::protobuf::RpcController *controller,
                    const ::curve::chunkserver::ResetPeerRequest *request,
                    ::curve::chunkserver::ResetPeerResponse *response,
                    ::google::protobuf::Closure *done);
    void snapshot(::google::protobuf::RpcController *controller,
                  const ::curve::chunkserver::SnapshotRequest *request,
                  ::curve::chunkserver::SnapshotResponse *response,
                  ::google::protobuf::Closure *done);
    void get_leader(::google::protobuf::RpcController *controller,
                    const ::curve::chunkserver::GetLeaderRequest *request,
                    ::curve::chunkserver::GetLeaderResponse *response,
                    ::google::protobuf::Closure *done);
    void change_peers(::google::protobuf::RpcController *controller,
                      const ::curve::chunkserver::ChangePeersRequest *request,
                      ::curve::chunkserver::ChangePeersResponse *response,
                      ::google::protobuf::Closure *done);
    void transfer_leader(::google::protobuf::RpcController *controller,
                         const ::curve::chunkserver::TransferLeaderRequest *request,
                         ::curve::chunkserver::TransferLeaderResponse *response,
                         ::google::protobuf::Closure *done);

 private:
    butil::Status get_node(scoped_refptr<braft::NodeImpl> *node,
                           const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const std::string &peer_id);
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_CLI_SERVICE_H

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

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class BRaftCliServiceImpl : public CliService {
 public:
    void add_peer(RpcController *controller,
                  const AddPeerRequest *request,
                  AddPeerResponse *response,
                  Closure *done);
    void remove_peer(RpcController *controller,
                     const RemovePeerRequest *request,
                     RemovePeerResponse *response,
                     Closure *done);
    void get_leader(RpcController *controller,
                    const GetLeaderRequest *request,
                    GetLeaderResponse *response,
                    Closure *done);
    void transfer_leader(RpcController *controller,
                         const TransferLeaderRequest *request,
                         TransferLeaderResponse *response,
                         Closure *done);

 private:
    butil::Status get_node(scoped_refptr<braft::NodeImpl> *node,
                           const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const std::string &peer_id);
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_CLI_SERVICE_H

/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_BRAFT_CLI_SERVICE_H_
#define SRC_CHUNKSERVER_BRAFT_CLI_SERVICE_H_

#include <butil/status.h>
#include <braft/node.h>

#include <string>

#include "src/chunkserver/copyset_node.h"
#include "proto/cli.pb.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

/**
 * braft配置变更Rpc Service
 */
class BRaftCliServiceImpl : public CliService {
 public:
    // 增加一个peer
    void add_peer(RpcController *controller,
                  const AddPeerRequest *request,
                  AddPeerResponse *response,
                  Closure *done);

    // 移除一个peer
    void remove_peer(RpcController *controller,
                     const RemovePeerRequest *request,
                     RemovePeerResponse *response,
                     Closure *done);

    // 获取copyset的leader
    void get_leader(RpcController *controller,
                    const GetLeaderRequest *request,
                    GetLeaderResponse *response,
                    Closure *done);

    // 转移leader
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

#endif  // SRC_CHUNKSERVER_BRAFT_CLI_SERVICE_H_

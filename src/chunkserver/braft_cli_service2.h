/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_BRAFT_CLI_SERVICE2_H_
#define SRC_CHUNKSERVER_BRAFT_CLI_SERVICE2_H_

#include <butil/status.h>
#include <braft/node.h>

#include <string>

#include "src/chunkserver/copyset_node.h"
#include "proto/cli2.pb.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

/**
 * braft配置变更Rpc Service
 */
class BRaftCliServiceImpl2 : public CliService2 {
 public:
    // 增加一个peer
    void AddPeer(RpcController *controller,
                 const AddPeerRequest2 *request,
                 AddPeerResponse2 *response,
                 Closure *done);

    // 移除一个peer
    void RemovePeer(RpcController *controller,
                    const RemovePeerRequest2 *request,
                    RemovePeerResponse2 *response,
                    Closure *done);

    // 变更配置
    void ChangePeers(RpcController *controller,
                     const ChangePeersRequest2 *request,
                     ChangePeersResponse2 *response,
                     Closure *done);

    // 获取copyset的leader
    void GetLeader(RpcController *controller,
                   const GetLeaderRequest2 *request,
                   GetLeaderResponse2 *response,
                   Closure *done);

    // 转移leader
    void TransferLeader(RpcController *controller,
                        const TransferLeaderRequest2 *request,
                        TransferLeaderResponse2 *response,
                        Closure *done);

    // 重置复制组成员
    void ResetPeer(RpcController* controller,
                   const ResetPeerRequest2* request,
                   ResetPeerResponse2* response,
                   Closure* done);

 private:
    /**
     * @brief: 查询指定的raft node
     * @param node[out]: 查询到的raft node
     * @param logicPoolId[in]: 逻辑池id
     * @param copysetId[in]: 复制组id
     * @param peer_id[in]: peer id
     * @return 成功，返回Status::OK()；否则，返回非Status::OK()
     */
    butil::Status get_node(scoped_refptr<braft::NodeImpl> *node,
                           const LogicPoolID &logicPoolId,
                           const CopysetID &copysetId,
                           const std::string &peer_id);
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_BRAFT_CLI_SERVICE2_H_

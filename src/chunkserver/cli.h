/*
 * Project: curve
 * Created Date: 18-8-27
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_CLI_H
#define CURVE_CHUNKSERVER_CLI_H

#include <braft/cli.h>
#include <braft/configuration.h>
#include <butil/status.h>

#include "src/chunkserver/copyset_node.h"

namespace curve {
namespace chunkserver {


// Cli 也就是配置变更相关接口的封装，方便使用，避免操作 RPC

butil::Status GetLeader(const LogicPoolID &logicPoolId, const CopysetID &copysetId, const Configuration &conf,
                        PeerId *leaderId);

butil::Status AddPeer(const LogicPoolID &logicPoolId, const CopysetID &copysetId, const Configuration &conf,
                      const PeerId &peer_id, const braft::cli::CliOptions &options);

butil::Status RemovePeer(const LogicPoolID &logicPoolId, const CopysetID &copysetId, const Configuration &conf,
                         const PeerId &peer_id, const braft::cli::CliOptions &options);

butil::Status TransferLeader(const LogicPoolID &logicPoolId, const CopysetID &copysetId, const Configuration &conf,
                             const PeerId &peer, const braft::cli::CliOptions &options);

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_CLI_H

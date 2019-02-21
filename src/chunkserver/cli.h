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

/**
 * Cli就是配置变更相关接口的封装，方便使用，避免直接操作RPC
 */

// 获取leader
butil::Status GetLeader(const LogicPoolID &logicPoolId,
                        const CopysetID &copysetId,
                        const Configuration &conf,
                        PeerId *leaderId);

// 增加一个peer
butil::Status AddPeer(const LogicPoolID &logicPoolId,
                      const CopysetID &copysetId,
                      const Configuration &conf,
                      const PeerId &peer_id,
                      const braft::cli::CliOptions &options);

// 移除一个peer
butil::Status RemovePeer(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId,
                         const Configuration &conf,
                         const PeerId &peer_id,
                         const braft::cli::CliOptions &options);

// 转移leader
butil::Status TransferLeader(const LogicPoolID &logicPoolId,
                             const CopysetID &copysetId,
                             const Configuration &conf,
                             const PeerId &peer,
                             const braft::cli::CliOptions &options);

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_CLI_H

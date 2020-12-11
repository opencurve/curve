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

#ifndef SRC_CHUNKSERVER_CLI2_H_
#define SRC_CHUNKSERVER_CLI2_H_

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
                        Peer *leader);

// 增加一个peer
butil::Status AddPeer(const LogicPoolID &logicPoolId,
                      const CopysetID &copysetId,
                      const Configuration &conf,
                      const Peer &peer,
                      const braft::cli::CliOptions &options);

// 移除一个peer
butil::Status RemovePeer(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId,
                         const Configuration &conf,
                         const Peer &peer,
                         const braft::cli::CliOptions &options);

// 变更配置
butil::Status ChangePeers(const LogicPoolID &logicPoolId,
                          const CopysetID &copysetId,
                          const Configuration &conf,
                          const Configuration &newPeers,
                          const braft::cli::CliOptions &options);

// 转移leader
butil::Status TransferLeader(const LogicPoolID &logicPoolId,
                             const CopysetID &copysetId,
                             const Configuration &conf,
                             const Peer &peer,
                             const braft::cli::CliOptions &options);

// 重置复制组
butil::Status ResetPeer(const LogicPoolID &logicPoolId,
                        const CopysetID &copysetId,
                        const Configuration& newPeers,
                        const Peer& requestPeer,
                        const braft::cli::CliOptions& options);

// 触发快照
butil::Status Snapshot(const LogicPoolID &logicPoolId,
                       const CopysetID &copysetId,
                       const Peer& peer,
                       const braft::cli::CliOptions& options);

// 给chunkserver上全部copyset副本触发快照
butil::Status SnapshotAll(const Peer& peer,
                          const braft::cli::CliOptions& options);

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CLI2_H_

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
 * Created Date: 2019-12-03
 * Author: lixiaocui
 */

#ifndef SRC_CHUNKSERVER_HEARTBEAT_HELPER_H_
#define SRC_CHUNKSERVER_HEARTBEAT_HELPER_H_

#include <braft/node_manager.h>
#include <vector>
#include <memory>
#include <string>
#include "proto/heartbeat.pb.h"
#include "src/chunkserver/copyset_node.h"

namespace curve {
namespace chunkserver {
using ::curve::mds::heartbeat::CopySetConf;
using ::curve::common::Peer;
using CopysetNodePtr = std::shared_ptr<CopysetNode>;

class HeartbeatHelper {
 public:
    /**
     * 根据mds下发的conf构建出指定复制组的新配置，给ChangePeer使用
     *
     * @param[in] conf mds下发的变更命令needupdatecopyset[i]
     * @param[out] newPeers 指定复制组的目标配置
     *
     * @return false-生成newpeers失败 true-生成newpeers成功
     */
    static bool BuildNewPeers(
        const CopySetConf &conf, std::vector<Peer> *newPeers);

    /**
     * 判断字符串peer(正确的形式为: ip:port:0)是否有效
     *
     * @param[in] peer 指定字符串
     *
     * @return false-无效 true-有效
     */
    static bool PeerVaild(const std::string &peer);

    /**
     * 判断mds下发过来的copysetConf是否合法，以下两种情况不合法:
     * 1. chunkserver中不存在该copyset
     * 2. mds下发的copyset中记录的epoch小于chunkserver上copyset此时的epoch
     *
     * @param[in] conf mds下发的变更命令needupdatecopyset[i]
     * @param[in] copyset chunkserver上对应的copyset
     *
     * @return false-copysetConf不合法，true-copysetConf合法
     */
    static bool CopySetConfValid(
        const CopySetConf &conf, const CopysetNodePtr &copyset);

    /**
     * 判断chunkserver(csEp)中指定copyset是否需要删除
     *
     * @param[in] csEp 该chunkserver的ip:port
     * @param[in] conf mds下发的变更命令needupdatecopyset[i]
     * @param[in] copyset chunkserver上对应的copyset
     *
     * @return false-该chunkserver上的copyset无需清理；
     *         true-该chunkserver上的copyset需要清理
     */
    static bool NeedPurge(const butil::EndPoint &csEp, const CopySetConf &conf,
        const CopysetNodePtr &copyset);

    /**
     * 判断指定chunkserver copyset是否已经加载完毕
     *
     * @return false-copyset加载完毕 true-copyset未加载完成
     */
    static bool ChunkServerLoadCopySetFin(const std::string ipPort);
};
}  // namespace chunkserver
}  // namespace curve
#endif  // SRC_CHUNKSERVER_HEARTBEAT_HELPER_H_


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
     * Build a new configuration for the specified replication group based on the conf issued by mds, and use it for ChangePeer
     *
     * @param[in] conf mds issued the change command needupdatecopyset[i]
     * @param[out] newPeers specifies the target configuration for the replication group
     *
     * @return false - Failed to generate newpeers, true - Successfully generated newpeers
     */
    static bool BuildNewPeers(
        const CopySetConf &conf, std::vector<Peer> *newPeers);

    /**
     * Determine whether the string peer (correct form: ip:port:0) is valid
     *
     * @param[in] peer specifies the string
     *
     * @return false - invalid, true - valid
     */
    static bool PeerVaild(const std::string &peer);

    /**
     * Determine whether the copysetConf sent by mds is legal, and the following two situations are illegal:
     * 1. The copyset does not exist in chunkserver
     * 2. The epoch recorded in the copyset issued by mds is smaller than the epoch recorded in the copyset on chunkserver at this time
     *
     * @param[in] conf mds issued the change command needupdatecopyset[i]
     * @param[in] copyset The corresponding copyset on chunkserver
     *
     * @return false-copysetConf is illegal, true-copysetConf is legal
     */
    static bool CopySetConfValid(
        const CopySetConf &conf, const CopysetNodePtr &copyset);

    /**
     * Determine whether the specified copyset in chunkserver(csEp) needs to be deleted
     *
     * @param[in] csEp The ip:port of this chunkserver
     * @param[in] conf mds issued the change command needupdatecopyset[i]
     * @param[in] copyset The corresponding copyset on chunkserver
     *
     * @return false-The copyset on the chunkserver does not need to be cleaned;
     *         true-The copyset on this chunkserver needs to be cleaned up
     */
    static bool NeedPurge(const butil::EndPoint &csEp, const CopySetConf &conf,
        const CopysetNodePtr &copyset);

    /**
     * Determine whether the specified chunkserver copyset has been loaded completely
     *
     * @return false-copyset loading completed, true-copyset not loaded completed
     */
    static bool ChunkServerLoadCopySetFin(const std::string ipPort);
};
}  // namespace chunkserver
}  // namespace curve
#endif  // SRC_CHUNKSERVER_HEARTBEAT_HELPER_H_


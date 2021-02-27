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
     * Build a new configuration of the specified copyset for ChangePeer based on the conf issued by mds
     *
     * @param[in] conf mds issued change command needupdatecopyset[i]
     * @param[out] newPeers specifies the target configuration of the replication group
     *
     * @return false-Failure to generate newpeers true-Successfully generate newpeers
     */
    static bool BuildNewPeers(
        const CopySetConf &conf, std::vector<Peer> *newPeers);

    /**
     * Determine if the string peer (correct form: ip:port:0) is valid
     *
     * @param[in] peer Specified string
     *
     * @return false-Invalid true-Valid
     */
    static bool PeerVaild(const std::string &peer);

    /**
     * Determine if the copysetConf sent from mds is legal, the following two cases are not legal:
     * 1. The copyset does not exist in the chunkserver
     * 2. The epoch recorded in the copyset sent by mds is less than the epoch of the copyset on the chunkserver
     *
     * @param[in] conf mds's change command needupdatecopyset[i]
     * @param[in] copyset The corresponding copyset on the chunkserver
     *
     * @return false-copysetConf illegal，true-copysetConf legal
     */
    static bool CopySetConfValid(
        const CopySetConf &conf, const CopysetNodePtr &copyset);

    /**
     * Determine if the specified copyset in chunkserver(csEp) needs to be deleted
     *
     * @param[in] csEp 该chunkserver的ip:port
     * @param[in] conf mds's change command needupdatecopyset[i]
     * @param[in] copyset The corresponding copyset on the chunkserver
     *
     * @return false-The copyset on this chunkserver does not need to be cleaned;
     *         true-The copyset on this chunkserver needs to be cleaned
     */
    static bool NeedPurge(const butil::EndPoint &csEp, const CopySetConf &conf,
        const CopysetNodePtr &copyset);

    /**
     * Determine if the specified chunkserver copyset has been loaded
     *
     * @return false-copyset has been loaded true-copyset has not been loaded
     */
    static bool ChunkServerLoadCopySetFin(const std::string ipPort);
};
}  // namespace chunkserver
}  // namespace curve
#endif  // SRC_CHUNKSERVER_HEARTBEAT_HELPER_H_


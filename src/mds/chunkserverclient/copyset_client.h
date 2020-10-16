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
 * Created Date: Fri Mar 08 2019
 * Author: xuchaojie
 */

#ifndef SRC_MDS_CHUNKSERVERCLIENT_COPYSET_CLIENT_H_
#define SRC_MDS_CHUNKSERVERCLIENT_COPYSET_CLIENT_H_

#include <memory>
#include "src/mds/common/mds_define.h"
#include "src/mds/topology/topology.h"

#include "src/mds/chunkserverclient/chunkserver_client.h"
#include "src/mds/chunkserverclient/chunkserverclient_config.h"
#include "src/common/channel_pool.h"

using ::curve::mds::topology::Topology;
using ::curve::mds::topology::CopySetInfo;

namespace curve {
namespace mds {
namespace chunkserverclient {

class CopysetClient {
 public:
     /**
      * @brief constructor
      *
      * @param topology
      */
    CopysetClient(std::shared_ptr<Topology> topo,
        const ChunkServerClientOption &option,
        std::shared_ptr<ChannelPool> channelPool)
        : topo_(topo),
          chunkserverClient_(
            std::make_shared<ChunkServerClient>(topo, option, channelPool)),
          updateLeaderRetryTimes_(option.updateLeaderRetryTimes),
          updateLeaderRetryIntervalMs_(option.updateLeaderRetryIntervalMs) {
    }

    void SetChunkServerClient(std::shared_ptr<ChunkServerClient> csClient) {
        chunkserverClient_ = csClient;
    }

    /**
     * @brief delete the snapshot generated during the dump or from history left
     *        over. If no snapshot is generated during the dump, modify the
     *        correctedSn of the chunk
     *
     * @param logicPoolId
     * @param copysetId
     * @param chunkId
     * @param correctedSn the version number that needs to be corrected when
     *                    there is no snapshot file in the chunk
     *
     * @return error code
     */
    int DeleteChunkSnapshotOrCorrectSn(LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t correctedSn);

    /**
     * @brief delete chunk files that are not snapshot files
     *
     * @param logicPoolId
     * @param copysetId
     * @param chunkId
     * @param sn file version number
     *
     * @return error code
     */
    int DeleteChunk(LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t sn);

    /**
     * @brief update leader
     *
     * @param[int][out] copyset
     *
     * @return error code
     */
    int UpdateLeader(CopySetInfo *copyset);

 private:
    std::shared_ptr<Topology> topo_;
    std::shared_ptr<ChunkServerClient> chunkserverClient_;

    uint32_t updateLeaderRetryTimes_;
    uint32_t updateLeaderRetryIntervalMs_;
};

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve


#endif  // SRC_MDS_CHUNKSERVERCLIENT_COPYSET_CLIENT_H_

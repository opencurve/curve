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

#ifndef SRC_MDS_CHUNKSERVERCLIENT_CHUNKSERVER_CLIENT_H_
#define SRC_MDS_CHUNKSERVERCLIENT_CHUNKSERVER_CLIENT_H_

#include <brpc/channel.h>
#include <butil/endpoint.h>

#include <memory>
#include <string>

#include "src/mds/common/mds_define.h"
#include "src/mds/topology/topology.h"
#include "proto/cli2.pb.h"
#include "proto/chunk.pb.h"
#include "src/mds/chunkserverclient/chunkserverclient_config.h"
#include "src/common/channel_pool.h"

using ::curve::mds::topology::Topology;
using ::curve::mds::topology::ChunkServerIdType;
using ::curve::common::ChannelPool;

namespace curve {
namespace mds {
namespace chunkserverclient {

class ChunkServerClient {
 public:
    ChunkServerClient(std::shared_ptr<Topology> topology,
        const ChunkServerClientOption &option,
        std::shared_ptr<ChannelPool> channelPool)
        : topology_(topology),
        option_(option),
        channelPool_(channelPool) {}

    virtual ~ChunkServerClient() {}

    /**
     * @brief  delete the snapshot generated during the dump or left from
     *         history. If no snapshot is generated during the dump,
     *         modify the correctedSn of the chunk
     *
     * @param leaderId
     * @param logicalPoolId
     * @param copysetId
     * @param chunkId chunk file ID
     * @param correctedSn CorrectedSn to be corrected when the snapshot chunk
     *                    does not exist
     *
     * @return error code
     */
    virtual int DeleteChunkSnapshotOrCorrectSn(ChunkServerIdType leaderId,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t correctedSn);

    /**
     * @brief delete chunk files that are not snapshot
     *
     * @param leaderId
     * @param logicalPoolId
     * @param copysetId
     * @param chunkId chunk file ID
     * @param sn file version number
     *
     * @return error code
     */
    virtual int DeleteChunk(ChunkServerIdType leaderId,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t sn);

    /**
     * @brief get the leader
     * @detail
     *   send a message to the target chunkserver to query the leader
     *
     * @param csId ID of target chunkserver
     * @param logicalPoolId
     * @param copysetId
     * @param[out] leader current leader
     *
     * @return error code
     */
    virtual int GetLeader(ChunkServerIdType csId,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkServerIdType * leader);

 private:
    /**
     * @brief get the address of the chunkserver from the topology
     *
     * @param csId ID of target chunkserver
     * @param[out] csAddr chunkserver address in 'ip:port' form
     *
     * @return error code
     */
    int GetChunkServerAddress(ChunkServerIdType csId,
                              std::string* csAddr);

    int GetOrInitChannel(ChunkServerIdType csId,
                         ChannelPtr* channelPtr);

    std::shared_ptr<Topology> topology_;
    ChunkServerClientOption option_;
    std::shared_ptr<ChannelPool> channelPool_;
};

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve


#endif  // SRC_MDS_CHUNKSERVERCLIENT_CHUNKSERVER_CLIENT_H_

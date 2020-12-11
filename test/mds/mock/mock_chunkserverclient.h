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
 * Created Date: Fri Mar 15 2019
 * Author: xuchaojie
 */

#ifndef TEST_MDS_MOCK_MOCK_CHUNKSERVERCLIENT_H_
#define TEST_MDS_MOCK_MOCK_CHUNKSERVERCLIENT_H_

#include <memory>
#include "src/mds/chunkserverclient/chunkserver_client.h"
#include "src/mds/chunkserverclient/chunkserverclient_config.h"


using ::curve::mds::topology::ChunkServerIdType;

namespace curve {
namespace mds {
namespace chunkserverclient {

class MockChunkServerClient : public ChunkServerClient {
 public:
    MockChunkServerClient(std::shared_ptr<Topology> topo,
        const ChunkServerClientOption &option,
        std::shared_ptr<ChannelPool> channelPool)
        : ChunkServerClient(topo, option, channelPool) {}
    MOCK_METHOD5(DeleteChunkSnapshotOrCorrectSn,
        int(ChunkServerIdType csId,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t sn));

    MOCK_METHOD5(DeleteChunk,
        int(ChunkServerIdType csId,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t sn));

    MOCK_METHOD4(GetLeader,
        int(ChunkServerIdType csId,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkServerIdType * leader));
};

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_MOCK_MOCK_CHUNKSERVERCLIENT_H_

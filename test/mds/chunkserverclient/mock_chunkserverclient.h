/*
 * Project: curve
 * Created Date: Fri Mar 15 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef TEST_MDS_CHUNKSERVERCLIENT_MOCK_CHUNKSERVERCLIENT_H_
#define TEST_MDS_CHUNKSERVERCLIENT_MOCK_CHUNKSERVERCLIENT_H_

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

#endif  // TEST_MDS_CHUNKSERVERCLIENT_MOCK_CHUNKSERVERCLIENT_H_

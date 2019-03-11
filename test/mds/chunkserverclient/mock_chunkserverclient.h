/*
 * Project: curve
 * Created Date: Fri Mar 15 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_TEST_MDS_CHUNKSERVERCLIENT_MOCK_CHUNKSERVERCLIENT_H_
#define CURVE_TEST_MDS_CHUNKSERVERCLIENT_MOCK_CHUNKSERVERCLIENT_H_

#include "src/mds/chunkserverclient/chunkserver_client.h"


using ::curve::mds::topology::ChunkServerIdType;

namespace curve {
namespace mds {
namespace chunkserverclient {

class MockChunkServerClient : public ChunkServerClient {
 public:
    explicit MockChunkServerClient(std::shared_ptr<Topology> topo)
        : ChunkServerClient(topo) {}
    MOCK_METHOD5(DeleteChunkSnapshot,
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

#endif  // CURVE_TEST_MDS_CHUNKSERVERCLIENT_MOCK_CHUNKSERVERCLIENT_H_

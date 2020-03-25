/*
 * Project: curve
 * Created Date: Sat Jan 05 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef TEST_MDS_MOCK_MOCK_COORDINATOR_H_
#define TEST_MDS_MOCK_MOCK_COORDINATOR_H_

#include <gmock/gmock.h>
#include <vector>
#include <map>
#include "src/mds/schedule/coordinator.h"

namespace curve {
namespace mds {
class MockCoordinator : public ::curve::mds::schedule::Coordinator {
 public:
    MockCoordinator() {}
    ~MockCoordinator() {}

    MOCK_METHOD3(CopySetHeartbeat, ChunkServerIDType(
                const ::curve::mds::topology::CopySetInfo &originInfo,
                const ::curve::mds::heartbeat::ConfigChangeInfo &configChInfo,
                ::curve::mds::heartbeat::CopySetConf *newConf));

    MOCK_METHOD2(ChunkserverGoingToAdd, bool(ChunkServerIDType, CopySetKey));

    MOCK_METHOD1(RapidLeaderSchedule, int(PoolIdType));

    MOCK_METHOD2(QueryChunkServerRecoverStatus,
        int(const std::vector<ChunkServerIdType> &,
            std::map<ChunkServerIdType, bool> *));
};
}  // namespace mds
}  // namespace curve
#endif  // TEST_MDS_MOCK_MOCK_COORDINATOR_H_


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
 * Created Date: Sat Jan 05 2019
 * Author: lixiaocui
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


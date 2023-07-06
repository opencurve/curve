/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-07-27
 * Author: wanghai (SeanHai)
 */

#ifndef TEST_MDS_MOCK_MOCK_HEARTBEAT_MANAGER_H_
#define TEST_MDS_MOCK_MOCK_HEARTBEAT_MANAGER_H_

#include <gmock/gmock.h>
#include "src/mds/heartbeat/heartbeat_manager.h"

namespace curve {
namespace mds {
namespace heartbeat {

class MockHeartbeatManager : public HeartbeatManager {
 public:
    MockHeartbeatManager() : HeartbeatManager(HeartbeatOption {},
        nullptr, nullptr, nullptr) {}
    ~MockHeartbeatManager() = default;
    MOCK_METHOD(void, ChunkServerHeartbeat,
    (const ChunkServerHeartbeatRequest &request,
    ChunkServerHeartbeatResponse *response), (override));
};

}  // namespace heartbeat
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_MOCK_MOCK_HEARTBEAT_MANAGER_H_

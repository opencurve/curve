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
 * Project: nebd
 * Created Date: Friday February 14th 2020
 * Author: yangyaokai
 */

#ifndef NEBD_TEST_PART2_MOCK_HEARTBEAT_MANAGER_H_
#define NEBD_TEST_PART2_MOCK_HEARTBEAT_MANAGER_H_

#include <gmock/gmock.h>
#include "nebd/src/part2/heartbeat_manager.h"

namespace nebd {
namespace server {
class MockHeartbeatManager : public HeartbeatManager {
 public:
    MockHeartbeatManager() : HeartbeatManager({0, 0, nullptr}) {}
    ~MockHeartbeatManager() {}
    MOCK_METHOD0(Init, int());
    MOCK_METHOD0(Fini, int());
    MOCK_METHOD2(UpdateFileTimestamp, bool(int, uint64_t));
};

}  // namespace server
}  // namespace nebd

#endif  // NEBD_TEST_PART2_MOCK_HEARTBEAT_MANAGER_H_

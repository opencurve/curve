/*
 * Project: nebd
 * Created Date: Friday February 14th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef TESTS_PART2_MOCK_HEARTBEAT_MANAGER_H_
#define TESTS_PART2_MOCK_HEARTBEAT_MANAGER_H_

#include <gmock/gmock.h>
#include "src/part2/heartbeat_manager.h"

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

#endif  // TESTS_PART2_MOCK_HEARTBEAT_MANAGER_H_

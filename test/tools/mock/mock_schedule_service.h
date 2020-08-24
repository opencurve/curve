/*
 * Project: curve
 * File Created: 2020-06-01
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#ifndef TEST_TOOLS_MOCK_MOCK_SCHEDULE_SERVICE_H_
#define TEST_TOOLS_MOCK_MOCK_SCHEDULE_SERVICE_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "proto/schedule.pb.h"

namespace curve {
namespace mds {
namespace schedule {
class MockScheduleService : public ScheduleService {
 public:
    MOCK_METHOD4(RapidLeaderSchedule,
        void(RpcController *controller,
        const RapidLeaderScheduleRequst *request,
        RapidLeaderScheduleResponse *response,
        Closure *done));
    MOCK_METHOD4(QueryChunkServerRecoverStatus,
        void(RpcController *controller,
        const QueryChunkServerRecoverStatusRequest *request,
        QueryChunkServerRecoverStatusResponse *response,
        Closure *done));
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_MOCK_SCHEDULE_SERVICE_H_

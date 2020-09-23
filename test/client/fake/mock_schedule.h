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
 * File Created: Monday, 8th October 2018 5:09:45 pm
 * Author: tongguangxun
 */

#ifndef TEST_CLIENT_FAKE_MOCK_SCHEDULE_H_
#define TEST_CLIENT_FAKE_MOCK_SCHEDULE_H_

#include <gmock/gmock.h>
#include <fiu.h>

#include <set>
#include <atomic>
#include <vector>
#include <string>
#include <thread>    // NOLINT
#include <chrono>    // NOLINT

#include "src/client/request_context.h"
#include "src/client/request_closure.h"
#include "src/client/request_scheduler.h"
#include "src/client/client_common.h"

using ::testing::_;
using ::testing::Invoke;

extern uint16_t sleeptimeMS;

class Schedule {
 public:
    Schedule() {
      enableScheduleFailed = false;
    }

    int ScheduleRequest(
        const std::vector<curve::client::RequestContext*>& reqlist);

    bool enableScheduleFailed;
};

class MockRequestScheduler : public curve::client::RequestScheduler {
 public:
    using REQ = std::vector<curve::client::RequestContext*>;
    MOCK_METHOD1(ScheduleRequest, int(const REQ&));

    void DelegateToFake() {
        ON_CALL(*this, ScheduleRequest(_))
            .WillByDefault(Invoke(&schedule, &Schedule::ScheduleRequest));
    }

    int Fini() {
       return 0;
    }

    void EnableScheduleFailed() {
       schedule.enableScheduleFailed = true;
    }

    void DisableScheduleFailed() {
       schedule.enableScheduleFailed = false;
    }

 private:
    Schedule schedule;
};

#endif  // TEST_CLIENT_FAKE_MOCK_SCHEDULE_H_

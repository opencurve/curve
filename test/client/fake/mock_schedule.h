/*
 * Project: curve
 * File Created: Monday, 8th October 2018 5:09:45 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef TEST_CLIENT_FAKE_MOCK_SCHEDULE_H_
#define TEST_CLIENT_FAKE_MOCK_SCHEDULE_H_

#include <gmock/gmock.h>
#include <fiu.h>

#include <set>
#include <atomic>
#include <list>
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
         const std::list<curve::client::RequestContext*> reqlist);

    bool enableScheduleFailed;
};

class MockRequestScheduler : public curve::client::RequestScheduler {
 public:
    using REQ = std::list<curve::client::RequestContext*>;
    MOCK_METHOD1(ScheduleRequest, int(const REQ));

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

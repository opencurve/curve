/*
 * Project: curve
 * File Created: Monday, 8th October 2018 5:09:45 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_LIBCURVE_MOCK_H
#define CURVE_LIBCURVE_MOCK_H

#include <gmock/gmock.h>

#include <set>
#include <atomic>
#include <list>
#include <string>

#include "src/client/request_context.h"
#include "src/client/request_closure.h"
#include "src/client/request_scheduler.h"
#include "src/client/client_common.h"

using ::testing::_;
using ::testing::Invoke;

class Schedule {
 public:
    int ScheduleRequest(
                const std::list<curve::client::RequestContext*>& reqlist);
};

class MockRequestScheduler : public curve::client::RequestScheduler {
 public:
    using REQ = std::list<curve::client::RequestContext*>;
    MOCK_METHOD1(ScheduleRequest, int(const REQ &));

    void DelegateToFake() {
        ON_CALL(*this, ScheduleRequest(_))
            .WillByDefault(Invoke(&schedule, &Schedule::ScheduleRequest));
    }

 private:
    Schedule schedule;
};

#endif  // !CURVE_LIBCURVE_IOSPLIT_CONTEXT_H

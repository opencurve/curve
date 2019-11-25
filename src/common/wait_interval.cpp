/*
 * Project: curve
 * Created Date: 20190805
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#include <thread> //NOLINT
#include <chrono> //NOLINT
#include "src/common/wait_interval.h"
#include "src/common/timeutility.h"

namespace curve {
namespace common {
void WaitInterval::Init(uint64_t intervalMs) {
    intevalMs_ = intervalMs;
    lastSend_ = 0;
}

void WaitInterval::WaitForNextExcution() {
    uint64_t now = TimeUtility::GetTimeofDayMs();
    if (lastSend_ == 0) {
        lastSend_ = TimeUtility::GetTimeofDayMs();
    }

    int rest = intevalMs_ - (now - lastSend_);
    rest = (rest < 0) ? 0 : rest;

    sleeper_.wait_for(std::chrono::milliseconds(rest));

    lastSend_ = TimeUtility::GetTimeofDayMs();
}

void WaitInterval::StopWait() {
    sleeper_.interrupt();
}
}  // namespace common
}  // namespace curve



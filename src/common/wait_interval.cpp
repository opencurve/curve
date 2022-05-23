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
 * Created Date: 20190805
 * Author: lixiaocui
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
    sleeper_.init();
    lastSend_ = TimeUtility::GetTimeofDayMs();
}

void WaitInterval::StopWait() {
    sleeper_.interrupt();
}
}  // namespace common
}  // namespace curve



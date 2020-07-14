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
 * Created Date: 2019-11-25
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "src/common/timeutility.h"

namespace curve {
namespace common {
InterruptibleSleeper sleeper;
bool is_stop = false;

void handler(int sig) {
    sleeper.interrupt();
}

TEST(InterruptibleSleeperTest, test_interruptible_sleeper) {
    pid_t pid = ::fork();
    if (0 > pid) {
        ASSERT_TRUE(false);
    } else if (0 == pid) {
        struct sigaction action;
        action.sa_handler = handler;
        sigemptyset(&action.sa_mask);
        action.sa_flags = 0;
        sigaction(SIGTERM, &action, NULL);

        while (sleeper.wait_for(std::chrono::seconds(10))) {}
        exit(0);
    }

    usleep(50 * 1000);
    uint64_t startKill = TimeUtility::GetTimeofDayMs();
    int waitstatus;
    kill(pid, SIGTERM);
    waitpid(pid, &waitstatus, 0);
    ASSERT_GT(8000, TimeUtility::GetTimeofDayMs() - startKill);
}
}  // namespace common
}  // namespace curve

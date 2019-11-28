/*
 * Project: curve
 * Created Date: 2019-11-25
 * Author: lixiaocui
 * Copyright (c) 2019 netease
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
        return;
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

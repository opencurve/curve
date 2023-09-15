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

#ifndef SRC_COMMON_INTERRUPTIBLE_SLEEPER_H_
#define SRC_COMMON_INTERRUPTIBLE_SLEEPER_H_

#include <chrono>  // NOLINT
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace common {
/**
 * Implement interruptible sleep functionality with InterruptibleSleeper.
 * Under normal circumstances, when wait_for times out and receives an exit signal,
 * the program will be immediately awakened, exit the while loop, and execute cleanup code.
 */
class InterruptibleSleeper {
 public:
    /**
     * @brief wait_for Wait for the specified time, and immediately return if an exit signal is received
     *
     * @param[in] time specifies the wait duration
     *
     * @return false - Received exit signal true - Exit after timeout
     */
    template<typename R, typename P>
    bool wait_for(std::chrono::duration<R, P> const& time) {
        UniqueLock lock(m);
        return !cv.wait_for(lock, time, [&]{return terminate;});
    }

    /**
     * @brief interrupt Send an exit signal to the current wait
     */
    void interrupt() {
        UniqueLock lock(m);
        terminate = true;
        cv.notify_all();
    }

    void init() {
        UniqueLock lock(m);
        terminate = false;
    }

 private:
    ConditionVariable cv;
    Mutex m;
    bool terminate = false;
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_INTERRUPTIBLE_SLEEPER_H_


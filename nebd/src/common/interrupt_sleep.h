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
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 */

#ifndef NEBD_SRC_COMMON_INTERRUPT_SLEEP_H_
#define NEBD_SRC_COMMON_INTERRUPT_SLEEP_H_

#include <signal.h>
#include <glog/logging.h>
#include <condition_variable>  // NOLINT

namespace nebd {
namespace common {

class InterruptibleSleeper {
 public:
    // returns false if killed:
    template<typename R, typename P>
    bool wait_for(std::chrono::duration<R, P> const& time ) {
        std::unique_lock<std::mutex> lock(m);
        return !cv.wait_for(lock, time, [&]{return terminate;});
    }

    void interrupt() {
        std::unique_lock<std::mutex> lock(m);
        terminate = true;
        LOG(WARNING) << "interrupt sleeper.";
        cv.notify_all();
    }
 private:
    std::condition_variable cv;
    std::mutex m;
    bool terminate = false;
};

}  // namespace common
}  // namespace nebd

#endif  // NEBD_SRC_COMMON_INTERRUPT_SLEEP_H_

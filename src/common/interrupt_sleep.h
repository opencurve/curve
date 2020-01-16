/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#ifndef SRC_COMMON_INTERRUPT_SLEEP_H_
#define SRC_COMMON_INTERRUPT_SLEEP_H_

#include <signal.h>
#include <butil/logging.h>
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
        LOG(ERROR) << "interrupt sleep.";
        cv.notify_all();
    }
 private:
    std::condition_variable cv;
    std::mutex m;
    bool terminate = false;
};

}  // namespace common
}  // namespace nebd

#endif  // SRC_COMMON_INTERRUPT_SLEEP_H_

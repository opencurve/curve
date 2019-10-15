/*
 * Project: nebd
 * File Created: 2019-09-30
 * Author: hzwuhongsong
 * Copyright (c) 2019 NetEase
 */

#ifndef SRC_PART2_INTERRUPT_SLEEP_H_
#define SRC_PART2_INTERRUPT_SLEEP_H_

#include <signal.h>
#include <butil/logging.h>
// #include <thread>
#include <condition_variable>  // NOLINT
// #include <mutex>
// #include <chrono>

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

#endif  // SRC_PART2_INTERRUPT_SLEEP_H_

/*
 * Project: curve
 * Created Date: 2019-11-25
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_COMMON_INTERRUPTIBLE_SLEEPER_H_
#define SRC_COMMON_INTERRUPTIBLE_SLEEPER_H_

#include <mutex>
#include <condition_variable>
#include <chrono>

namespace curve {
namespace common {
class InterruptibleSleeper {
 public:
    template<typename R, typename P>
    bool wait_for(std::chrono::duration<R, P> const& time) {
        std::unique_lock<std::mutex> lock(m);
        return !cv.wait_for(lock, time, [&]{return terminate;});
    }

    void interrupt() {
        std::unique_lock<std::mutex> lock(m);
        terminate = true;
        cv.notify_all();
    }

 private:
    std::condition_variable cv;
    std::mutex m;
    bool terminate = false;
};

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_INTERRUPTIBLE_SLEEPER_H_


/*
 * Project: curve
 * File Created: Friday, 21st September 2018 11:21:15 am
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_LIBCURVE_IO_CONDITION_VARAIABLE_H
#define CURVE_LIBCURVE_IO_CONDITION_VARAIABLE_H

#include <condition_variable>   //NOLINT
#include <mutex>    //NOLINT

namespace curve {
namespace client {
class IOCond {
 public:
    IOCond() {
        Reset();
    }
    ~IOCond() {
    }

    void Complete(int r) {
        std::unique_lock<std::mutex> lk(mtx_);
        ret = r;
        done_ = true;
        cv_.notify_one();
    }

    int Wait() {
        std::unique_lock<std::mutex> lk(mtx_);
        cv_.wait(lk, [&]()->bool {return done_;});
        return ret;
    }

    void Reset() {
        ret = -1;
        done_ = false;
    }

 private:
    int     ret;
    bool    done_;
    std::mutex  mtx_;
    std::condition_variable cv_;
};
}   // namespace client
}   // namespace curve
#endif  // !CURVE_LIBCURVE_IO_CONDITION_VARAIABLE_H

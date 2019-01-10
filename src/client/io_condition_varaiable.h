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
// IOConditionVariable是用户同步IO场景下IO等待条件变量
class IOConditionVariable {
 public:
    IOConditionVariable() {
        ret = -1;
        done_ = false;
    }
    ~IOConditionVariable() = default;

    /**
     * 条件变量唤醒函数，因为底层的RPC request是异步的，所以用户下发同步IO的时候需要
     * 在发送读写请求的时候暂停等待IO返回。
     * @param: retcode是当前IO的返回值
     */
    void Complete(int retcode) {
        std::unique_lock<std::mutex> lk(mtx_);
        ret = retcode;
        done_ = true;
        cv_.notify_one();
    }

    /**
     * 是用户IO需要等待时候调用的函数，这个函数会在Complete被调用的时候返回
     */
    int  Wait() {
        std::unique_lock<std::mutex> lk(mtx_);
        cv_.wait(lk, [&]()->bool {return done_;});
        done_ = false;
        return ret;
    }

 private:
    // 当前IO的返回值
    int     ret;

    // 当前IO是否完成
    bool    done_;

    // 条件变量使用的锁
    std::mutex  mtx_;

    // 条件变量用于等待
    std::condition_variable cv_;
};
}   // namespace client
}   // namespace curve
#endif  // !CURVE_LIBCURVE_IO_CONDITION_VARAIABLE_H

/*
 * Project: curve
 * Created Date: 18-9-26
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CLIENT_THREAD_POOL_H
#define CURVE_CLIENT_THREAD_POOL_H

#include <functional>
#include <thread>   //NOLINT
#include <vector>
#include <mutex>    //NOLINT
#include <atomic>

#include "src/common/uncopyable.h"

namespace curve {
namespace common {

class ThreadPool : public Uncopyable {
 public:
    ThreadPool();
    ~ThreadPool();

    int Init(int numThreads, std::function<void()> func);
    void Start();
    void Stop();
    int NumOfThreads();

 private:
    std::vector<std::unique_ptr<std::thread>> threads_;
    int numThreads_;
    std::function<void()> threadFunc_;
    std::atomic<bool> starting_;
};

}  // namespace common
}  // namespace curve

#endif  // CURVE_CLIENT_THREAD_POOL_H

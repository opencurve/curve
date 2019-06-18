/*
 * Project: curve
 * Created Date: 18-9-26
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_COMMON_CONCURRENT_THREAD_POOL_H_
#define SRC_COMMON_CONCURRENT_THREAD_POOL_H_

#include <functional>
#include <thread>   //NOLINT
#include <vector>
#include <mutex>    //NOLINT
#include <atomic>
#include <memory>

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

#endif  // SRC_COMMON_CONCURRENT_THREAD_POOL_H_

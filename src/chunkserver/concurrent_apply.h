/*
 * Project: curve
 * File Created: Tuesday, 11th December 2018 6:26:24 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVE_CHUNKERVER_CONCURRENT_H
#define CURVE_CHUNKERVER_CONCURRENT_H

#include <glog/logging.h>
#include <unistd.h>
#include <atomic>
#include <mutex>    // NOLINT
#include <thread>    // NOLINT
#include <unordered_map>
#include <condition_variable>    // NOLINT

#include "src/common/task_queue.h"
#include "include/curve_compiler_specific.h"

using curve::common::TaskQueue;
namespace curve {
namespace chunkserver {

class CURVE_CACHELINE_ALIGNMENT ConcurrentApplyModule {
 public:
    ConcurrentApplyModule();
    ~ConcurrentApplyModule();
    /**
     * @param: concurrentsize是当前并发模块的并发大小
     * @param: queuedepth是当前并发模块每个队列的深度控制
     */
    bool Init(int concurrentsize, int queuedepth);

    /**
     * raft apply线程会将task push到后台队列
     * @param: key用于将task哈希到指定队列
     * @param: f为要执行的task
     * @param: args为执行task的参数
     */
    template<class F, class... Args>
    bool Push(uint64_t key, F&& f, Args&&... args) {
        if (!isStarted_) {
            LOG(WARNING) << "concurrent module not start!";
            return false;
        }

        auto task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
        applypoolMap_[Hash(key)]->tq.Push(task);
        return true;
    };                                                                                  // NOLINT

    // raft snapshot之前需要将队列中的IO全部落盘。
    void Flush();
    void Stop();

 private:
    void Run(int index);
    inline int Hash(uint64_t key) {
        return key % concurrentsize_;
    }

 private:
    typedef uint8_t threadIndex;
    typedef struct taskthread {
        std::thread th;
        TaskQueue tq;
        taskthread(size_t capacity):tq(capacity) {}
        ~taskthread() = default;
    } taskthread_t;

    // 常规的stop和start控制变量
    bool stop_;
    bool isStarted_;
    // 每个队列的深度
    int queuedepth_;
    // 并发度
    int concurrentsize_;
    // 用于统一启动后台线程的锁和条件变量
    std::mutex startmtx_;
    std::condition_variable startcv_;
    // 存储threadindex与taskthread的映射关系
    CURVE_CACHELINE_ALIGNMENT std::unordered_map<threadIndex, taskthread_t*> applypoolMap_;     // NOLINT
};
}   // namespace chunkserver
}   // namespace curve

#endif  // !CURVE_CHUNKERVER_CONCURRENT_H

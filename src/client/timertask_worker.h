/*
 * Project: curve
 * File Created: Thursday, 20th December 2018 10:55:26 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_TIMERTASK_WORKER_H_
#define SRC_CLIENT_TIMERTASK_WORKER_H_

#include <functional>
#include <thread>   // NOLINT
#include <map>
#include <chrono>   // NOLINT
#include <mutex>    // NOLINT
#include <atomic>
#include <utility>
#include <condition_variable>   // NOLINT
#include "src/common/timeutility.h"

using curve::common::TimeUtility;
typedef std::chrono::microseconds   MicroSeconds;
typedef std::chrono::steady_clock   SteadyClock;
typedef std::chrono::steady_clock::time_point TimePoint;

namespace curve {
namespace client {
/**
 * TimerTask是一个定时任务，添加到timetask worker定时执行
 */
class TimerTask {
 public:
    /**
     * 构造函数
     * @param: lease是当前task的定时时间
     */
    explicit TimerTask(uint64_t lease) {
        deleteself_ = false;
        leasetimeus = lease;
        timerid = id_.fetch_add(1, std::memory_order_relaxed);
    }
    ~TimerTask() = default;
    /**
     * 拷贝构造函数
     */
    TimerTask(const TimerTask& other) {
        this->leasetimeus = other.leasetimeus;
        this->task = std::move(other.task);
        this->timerid = other.timerid;
    }
    /**
     * 赋值构造函数
     */
    TimerTask& operator=(const TimerTask& other) {
        this->leasetimeus = other.leasetimeus;
        this->task = std::move(other.task);
        this->timerid = other.timerid;
        return *this;
    }
    /**
     * 添加对应的定时器执行函数
     * @param: f是执行函数
     * @param: args是执行函数的参数
     */
    template<class F, class... Args>
    void AddCallback(F&& f, Args&&... args) {
        task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    };  // NOLINT

    /**
     * 获取定时时间
     */
    uint64_t LeaseTime() const {
        return leasetimeus;
    }
    /**
     * 当前task是否有效
     */
    bool Valid() {
        return task != nullptr;
    }
    /**
     * 执行task
     */
    void Run() {
        task();
    }
    /**
     * 是否将自己从定时器中删除
     */
    bool DeleteSelf() {
        return deleteself_;
    }
    /**
     * 设置从定时器worker中删除
     */
    void SetDeleteSelf() {
        deleteself_ = true;
    }
    /**
     * 获取timerid信息
     */
    uint64_t GetTimerID() {
        return timerid;
    }
    /**
     * 判断两个task是否相等
     */
    inline bool operator== (const TimerTask& t) {
        return this->timerid == t.timerid;
    }

 private:
    // 从定时其worker中删除标志
    bool        deleteself_;

    // 当前task的id
    uint64_t    timerid;

    // 当前task的定时时长
    uint64_t    leasetimeus;

    // 定时器执行函数
    std::function<void()> task;

    // 全局id，每创建一个task就自增
    static std::atomic<uint64_t>  id_;
};

// 执行定时任务的工作线程
class TimerTaskWorker {
 public:
    TimerTaskWorker();
    ~TimerTaskWorker() = default;
    /**
     * 启动
     */
    void Start();
    /**
     * 停止
     */
    void Stop();
    /**
     * 添加定时任务
     * @param: tt是待添加的任务
     * @return: 成功返回true、否则返回false
     */
    bool AddTimerTask(TimerTask* tt);
    /**
     * 取消定时任务
     * @param: tt是待取消的任务
     * @return: 成功返回true、否则返回false
     */
    bool CancelTimerTask(const TimerTask* tt);
    /**
     * task是否在定时器池子中
     * @param: 要查询的定时器任务
     * @return: 在则返回true，不在返回false
     */
    bool HasTimerTask(const TimerTask* tt);

 private:
    typedef std::multimap<TimePoint, TimerTask*>::iterator TaskIter;
    // timertask是否在worker中,返回该task在定时器池子中的迭代器
    TaskIter TimerTaskIn(const TimerTask* tt);
    // 线程函数
    void Run();

    // 停止线程函数
    std::atomic<bool> stop_;

    // 当前线程是否启动
    bool isstarted_;

    // 定时器执行线程
    std::thread th_;

    // 定时器容器锁
    std::mutex datamtx_;

    // 睡眠器，隔一段时间醒来执行定时器
    std::mutex sleepmtx_;

    // 定时器容器
    std::multimap<TimePoint, TimerTask*> taskmap_;

    // 非空条件变量
    std::condition_variable notemptycv_;

    // 睡眠条件变量
    std::condition_variable sleepcv_;
};
}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_TIMERTASK_WORKER_H_

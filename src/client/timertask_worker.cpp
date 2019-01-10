/*
 * Project: curve
 * File Created: Thursday, 20th December 2018 10:55:40 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#include <glog/logging.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <climits>
#include <chrono>   // NOLINT
#include <algorithm>
#include "src/client/timertask_worker.h"

namespace curve {
namespace client {
std::atomic<uint64_t> TimerTask::id_(1);

TimerTaskWorker::TimerTaskWorker():
                stop_(false)
                , isstarted_(false) {
    taskmap_.clear();
}

void TimerTaskWorker::Start() {
    if (isstarted_) {
        LOG(WARNING) << "timer task worker already started!";
        return;
    }

    th_ = std::move(std::thread(&TimerTaskWorker::Run, this));
    isstarted_ = true;
    return;
}

bool TimerTaskWorker::AddTimerTask(TimerTask* tt) {
    std::unique_lock<std::mutex> lk(datamtx_);
    TaskIter iter = TimerTaskIn(tt);
    if (iter != taskmap_.end()) {
        return false;
    }
    TimePoint key = MicroSeconds(tt->LeaseTime()) + SteadyClock::now();
    taskmap_.insert(std::make_pair(key, tt));
    notemptycv_.notify_one();
    return true;
}

bool TimerTaskWorker::CancelTimerTask(const TimerTask* tt) {
    std::unique_lock<std::mutex> lk(datamtx_);
    TaskIter iter = TimerTaskIn(tt);
    if (iter != taskmap_.end()) {
        taskmap_.erase(iter);
        return true;
    }
    return false;
}

bool TimerTaskWorker::HasTimerTask(const TimerTask* tt) {
    std::unique_lock<std::mutex> lk(datamtx_);
    TaskIter iter = TimerTaskIn(tt);
    if (iter != taskmap_.end()) {
        return true;
    }
    return false;
}

TimerTaskWorker::TaskIter TimerTaskWorker::TimerTaskIn(const TimerTask* tt) {
    return std::find_if(taskmap_.begin(), taskmap_.end(),
        [&tt](std::pair<TimePoint, TimerTask*> it){
        return *it.second == *tt;
    });
}

void TimerTaskWorker::Run() {
    TimePoint nextwakeuptime;
    std::multimap<TimePoint, TimerTask*> tempmap;
    tempmap.clear();
    while (!stop_.load(std::memory_order_acquire)) {
        if (taskmap_.empty()) {
            std::unique_lock<std::mutex> lk(emptymtx_);
            notemptycv_.wait(lk, [this]()->bool{return !this->taskmap_.empty();});  // NOLINT
        }

        std::unique_lock<std::mutex> sleeplk(sleepmtx_);
        sleepcv_.wait_until(sleeplk, nextwakeuptime);
        auto ret = SteadyClock::now();

        std::unique_lock<std::mutex> lk(datamtx_);
        auto iter = taskmap_.begin();
        for (; iter != taskmap_.end(); ) {
            if (iter->first > ret) {
                break;
            } else {
                if (iter->second->Valid()) {
                    iter->second->Run();
                }
                auto key = iter->first + MicroSeconds(iter->second->LeaseTime());   // NOLINT
                if (!iter->second->DeleteSelf()) {
                    tempmap.insert(std::make_pair(key, iter->second));
                } else {
                    LOG(INFO) << "timer " << iter->second->GetTimerID() << ", deleted!";    // NOLINT
                }
                iter = taskmap_.erase(iter);
                continue;
            }
            iter++;
        }
        taskmap_.insert(tempmap.begin(), tempmap.end());
        nextwakeuptime = taskmap_.begin()->first;
        tempmap.clear();
    }
    taskmap_.clear();
}

void TimerTaskWorker::Stop() {
    stop_.store(true);
    TimerTask* tt = new (std::nothrow) TimerTask(0);

    AddTimerTask(tt);
    notemptycv_.notify_one();

    sleepcv_.notify_one();
    if (th_.joinable()) {
        th_.join();
    }

    delete tt;
    isstarted_ = false;
}
}   // namespace client
}   // namespace curve

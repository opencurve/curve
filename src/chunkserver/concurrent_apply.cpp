/*
 * Project: curve
 * File Created: Tuesday, 11th December 2018 6:26:14 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <glog/logging.h>

#include <algorithm>
#include "src/chunkserver/concurrent_apply.h"

namespace curve {
namespace chunkserver {

#define DEFAULT_CONCURRENT_SIZE 10
#define DEFAULT_QUEUEDEPTH 1

ConcurrentApplyModule::ConcurrentApplyModule():
                                    stop_(0),
                                    isStarted_(false),
                                    concurrentsize_(0),
                                    queuedepth_(0) {
    applypoolMap_.clear();
}

ConcurrentApplyModule::~ConcurrentApplyModule() {
}

bool ConcurrentApplyModule::Init(int concurrentsize, int queuedepth) {
    if (isStarted_) {
        LOG(WARNING) << "concurrent module already start!";
        return true;
    }

    if (concurrentsize <= 0) {
        concurrentsize_ = DEFAULT_CONCURRENT_SIZE;
    } else {
        concurrentsize_ = concurrentsize;
    }

    if (queuedepth <= 0) {
        queuedepth_ = DEFAULT_QUEUEDEPTH;
    } else {
        queuedepth_ = queuedepth;
    }

    for (int i = 0; i < concurrentsize_; i++) {
        auto asyncth = new (std::nothrow) taskthread(queuedepth_);
        CHECK(asyncth != nullptr) << "allocate failed!";
        applypoolMap_.insert(std::make_pair(i, asyncth));
        asyncth->th = std::move(std::thread(&ConcurrentApplyModule::Run, this, i));     // NOLINT
    }

    isStarted_ = true;
    startcv_.notify_all();
    return isStarted_;
}

void ConcurrentApplyModule::Run(int index) {
    {
        std::unique_lock<std::mutex> lk(startmtx_);
        startcv_.wait(lk, [this]()->bool {return this->isStarted_;});
    }
    while (!stop_) {
        auto t = applypoolMap_[index]->tq.Pop();
        t();
    }
}

void ConcurrentApplyModule::Stop() {
    stop_ = true;

    auto wakeup = []() {};
    for (auto iter : applypoolMap_) {
        iter.second->tq.Push(wakeup);
        iter.second->th.join();
        delete iter.second;
    }
    applypoolMap_.clear();

    isStarted_ = false;
}

void ConcurrentApplyModule::Flush() {
    if (!isStarted_) {
        LOG(WARNING) << "concurrent module not start!";
        return;
    }

    std::atomic<bool>* signal = new (std::nothrow) std::atomic<bool>[concurrentsize_];          //NOLINT
    std::mutex* mtx = new (std::nothrow) std::mutex[concurrentsize_];
    std::condition_variable* cv= new (std::nothrow) std::condition_variable[concurrentsize_];   //NOLINT
    CHECK(signal != nullptr && mtx != nullptr && cv != nullptr)
    << "allocate buffer failed!";

    for (int i = 0; i < concurrentsize_; i++) {
        signal[i].store(false);
    }

    auto flushtask = [&mtx, &signal, &cv](int i) {
        {
            std::unique_lock<std::mutex> lk(mtx[i]);
            signal[i].store(true);
        }
        cv[i].notify_one();
    };

    auto flushwait = [&mtx, &signal, &cv](int i) {
        std::unique_lock<std::mutex> lk(mtx[i]);
        cv[i].wait(lk, [&]()->bool{return signal[i].load();});
    };

    for (int i = 0; i < concurrentsize_; i++) {
        applypoolMap_[i]->tq.Push(flushtask, i);
    }

    for (int i = 0; i < concurrentsize_; i++) {
        flushwait(i);
    }

    delete[] signal;
    delete[] mtx;
    delete[] cv;
}

}   // namespace chunkserver
}   // namespace curve

/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * File Created: Tuesday, 11th December 2018 6:26:14 pm
 * Author: tongguangxun
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
                                    queuedepth_(0),
                                    cond_(0) {
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

    // 等待event事件数，等于线程数
    cond_.Reset(concurrentsize);

    /**
     * 因为hash map并不是线程安全的，所以必须先将applyPoolMap_创建好
     * 然后插入所有元素之后，之后才能创建线程，这样对applypoolMap_的
     * read/write就不可能出现并发
     */
    for (int i = 0; i < concurrentsize_; i++) {
        auto asyncth = new (std::nothrow) taskthread(queuedepth_);
        CHECK(asyncth != nullptr) << "allocate failed!";
        applypoolMap_.insert(std::make_pair(i, asyncth));
    }

    for (int i = 0; i < concurrentsize_; i++) {
        applypoolMap_[i]->th = std::move(std::thread(&ConcurrentApplyModule::Run, this, i));     // NOLINT
    }

    /**
     * 等待所有线程创建完成，默认等待5秒，后台线程还没有全部创建成功，
     * 那么可以认为系统或者程序出现了问题，可以判定这次init失败了，直接退出
     */
    if (cond_.WaitFor(5000)) {
        isStarted_ = true;
    } else {
        LOG(ERROR) << "init concurrent module's threads fail";
        isStarted_ = false;
    }

    return isStarted_;
}

void ConcurrentApplyModule::Run(int index) {
    cond_.Signal();
    while (!stop_) {
        auto t = applypoolMap_[index]->tq.Pop();
        t();
    }
}

void ConcurrentApplyModule::Stop() {
    LOG(INFO) << "stop ConcurrentApplyModule...";
    stop_ = true;
    auto wakeup = []() {};
    for (auto iter : applypoolMap_) {
        iter.second->tq.Push(wakeup);
        iter.second->th.join();
        delete iter.second;
    }
    applypoolMap_.clear();

    isStarted_ = false;
    LOG(INFO) << "stop ConcurrentApplyModule ok.";
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
        std::unique_lock<std::mutex> lk(mtx[i]);
        signal[i].store(true);
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

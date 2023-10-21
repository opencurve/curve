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
 * File Created: 20200813
 * Author: lixiaocui
 */

#include <glog/logging.h>

#include <algorithm>
#include "src/chunkserver/concurrent_apply/concurrent_apply.h"
#include "src/common/concurrent/count_down_event.h"

using ::curve::common::CountDownEvent;

namespace curve {
namespace chunkserver {
namespace concurrent {
bool ConcurrentApplyModule::Init(const ConcurrentApplyOption &opt) {
    if (start_) {
        LOG(WARNING) << "concurrent module already start!";
        return true;
    }

    if (false == checkOptAndInit(opt)) {
        return false;
    }

    start_ = true;
    cond_.Reset(opt.rconcurrentsize + opt.wconcurrentsize);
    InitThreadPool(ApplyTaskType::READ, rconcurrentsize_, rqueuedepth_);
    InitThreadPool(ApplyTaskType::WRITE, wconcurrentsize_, wqueuedepth_);

    if (!cond_.WaitFor(5000)) {
        LOG(ERROR) << "init concurrent module's threads fail";
        start_ = false;
    }

    LOG(INFO) << "Init concurrent module's threads success";
    return start_;
}

bool ConcurrentApplyModule::checkOptAndInit(
    const ConcurrentApplyOption &opt) {
    if (opt.rconcurrentsize <= 0 || opt.wconcurrentsize <= 0 ||
        opt.rqueuedepth <= 0 || opt.wqueuedepth <= 0) {
        LOG(INFO) << "init concurrent module fail, params must >=0"
            << ", rconcurrentsize=" << opt.rconcurrentsize
            << ", wconcurrentsize=" << opt.wconcurrentsize
            << ", rqueuedepth=" << opt.rqueuedepth
            << ", wconcurrentsize=" << opt.wqueuedepth;
        return false;
    }

    wconcurrentsize_ = opt.wconcurrentsize;
    wqueuedepth_ = opt.wqueuedepth;
    rconcurrentsize_ = opt.rconcurrentsize;
    rqueuedepth_ = opt.rqueuedepth;

    return true;
}


void ConcurrentApplyModule::InitThreadPool(
    ApplyTaskType type, int concurrent, int depth) {
    for (int i = 0; i < concurrent; i++) {
        auto asyncth = new (std::nothrow) TaskThread(depth);
        CHECK(asyncth != nullptr) << "allocate failed!";

        switch (type) {
        case ApplyTaskType::READ:
            rapplyMap_.insert(std::make_pair(i, asyncth));
            break;

        case ApplyTaskType::WRITE:
            wapplyMap_.insert(std::make_pair(i, asyncth));
            break;
        }
    }

    for (int i = 0; i < concurrent; i++) {
        switch (type) {
        case ApplyTaskType::READ:
            rapplyMap_[i]->th =
                    std::thread(&ConcurrentApplyModule::Run, this, type, i);
            break;

        case ApplyTaskType::WRITE:
            wapplyMap_[i]->th =
                    std::thread(&ConcurrentApplyModule::Run, this, type, i);
            break;
        }
    }
}

void ConcurrentApplyModule::Run(ApplyTaskType type, int index) {
    cond_.Signal();
    while (start_) {
        switch (type) {
        case ApplyTaskType::READ:
            rapplyMap_[index]->tq.Pop()();
            break;

        case ApplyTaskType::WRITE:
            wapplyMap_[index]->tq.Pop()();
            break;
        }
    }
}

void ConcurrentApplyModule::Stop() {
    if (!start_.exchange(false)) {
        return;
    }

    LOG(INFO) << "stop ConcurrentApplyModule...";
    auto wakeup = []() {};
    for (auto iter : rapplyMap_) {
        iter.second->tq.Push(wakeup);
        iter.second->th.join();
        delete iter.second;
    }
    rapplyMap_.clear();

    for (auto iter : wapplyMap_) {
        iter.second->tq.Push(wakeup);
        iter.second->th.join();
        delete iter.second;
    }
    wapplyMap_.clear();

    LOG(INFO) << "stop ConcurrentApplyModule ok.";
}

void ConcurrentApplyModule::Flush() {
    if (!start_.load(std::memory_order_relaxed)) {
        return;
    }

    CountDownEvent event(wconcurrentsize_);
    auto flushtask = [&event]() { event.Signal(); };

    for (int i = 0; i < wconcurrentsize_; i++) {
        wapplyMap_[i]->tq.Push(flushtask);
    }

    event.Wait();
}

void ConcurrentApplyModule::FlushAll() {
    if (!start_.load(std::memory_order_relaxed)) {
        return;
    }

    CountDownEvent event(wconcurrentsize_ + rconcurrentsize_);
    auto flushtask = [&event]() { event.Signal(); };

    for (int i = 0; i < wconcurrentsize_; i++) {
        wapplyMap_[i]->tq.Push(flushtask);
    }

    for (int i = 0; i < rconcurrentsize_; i++) {
        rapplyMap_[i]->tq.Push(flushtask);
    }

    event.Wait();
}

}   // namespace concurrent
}   // namespace chunkserver
}   // namespace curve

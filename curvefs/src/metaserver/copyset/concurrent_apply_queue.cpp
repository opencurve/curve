/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * File Created: 20230521
 * Author: Xinlong-Chen
 */


#include "curvefs/src/metaserver/copyset/concurrent_apply_queue.h"

#include <algorithm>

namespace curvefs {
namespace metaserver {
namespace copyset {
bool ApplyQueue::Init(const ApplyOption &opt) {
    if (start_) {
        LOG(WARNING) << "concurrent module already start!";
        return true;
    }

    if (false == CheckOptAndInit(opt)) {
        return false;
    }

    start_ = true;
    cond_.Reset(opt.rconcurrentsize + opt.wconcurrentsize);
    InitThreadPool(ThreadPoolType::READ, rconcurrentsize_, rqueuedepth_);
    InitThreadPool(ThreadPoolType::WRITE, wconcurrentsize_, wqueuedepth_);

    if (!cond_.WaitFor(5000)) {
        LOG(ERROR) << "init concurrent module's threads fail";
        start_ = false;
    }

    LOG(INFO) << "Init concurrent module's threads success";
    return start_;
}

bool ApplyQueue::CheckOptAndInit(const ApplyOption &opt) {
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

void ApplyQueue::InitThreadPool(
    ThreadPoolType type, int concurrent, int depth) {
    for (int i = 0; i < concurrent; i++) {
        auto asyncth = new (std::nothrow) TaskThread(depth);
        CHECK(asyncth != nullptr) << "allocate failed!";

        switch (type) {
        case ThreadPoolType::READ:
            rapplyMap_.insert(std::make_pair(i, asyncth));
            break;

        case ThreadPoolType::WRITE:
            wapplyMap_.insert(std::make_pair(i, asyncth));
            break;
        }
    }

    for (int i = 0; i < concurrent; i++) {
        switch (type) {
        case ThreadPoolType::READ:
            rapplyMap_[i]->th =
                    std::thread(&ApplyQueue::Run, this, type, i);
            break;

        case ThreadPoolType::WRITE:
            wapplyMap_[i]->th =
                    std::thread(&ApplyQueue::Run, this, type, i);
            break;
        }
    }
}

void ApplyQueue::Run(ThreadPoolType type, int index) {
    cond_.Signal();
    while (start_) {
        switch (type) {
        case ThreadPoolType::READ:
            rapplyMap_[index]->tq.Pop()();
            break;

        case ThreadPoolType::WRITE:
            wapplyMap_[index]->tq.Pop()();
            break;
        }
    }
}

void ApplyQueue::Stop() {
    if (!start_.exchange(false)) {
        return;
    }

    LOG(INFO) << "stop ApplyQueue...";
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

    LOG(INFO) << "stop ApplyQueue ok.";
}

void ApplyQueue::Flush() {
    if (!start_.load(std::memory_order_relaxed)) {
        return;
    }

    CountDownEvent event(wconcurrentsize_);
    auto flushtask = [&event]() {
        event.Signal();
    };

    for (int i = 0; i < wconcurrentsize_; i++) {
        wapplyMap_[i]->tq.Push(flushtask);
    }

    event.Wait();
}

void ApplyQueue::FlushAll() {
    if (!start_.load(std::memory_order_relaxed)) {
        return;
    }

    CountDownEvent event(wconcurrentsize_ + rconcurrentsize_);
    auto flushtask = [&event]() {
        event.Signal();
    };

    for (int i = 0; i < wconcurrentsize_; i++) {
        wapplyMap_[i]->tq.Push(flushtask);
    }

    for (int i = 0; i < rconcurrentsize_; i++) {
        rapplyMap_[i]->tq.Push(flushtask);
    }

    event.Wait();
}

ThreadPoolType ApplyQueue::Schedule(OperatorType optype) {
    switch (optype) {
    case OperatorType::GetDentry:
    case OperatorType::ListDentry:
    case OperatorType::GetInode:
    case OperatorType::BatchGetInodeAttr:
    case OperatorType::BatchGetXAttr:
    case OperatorType::GetVolumeExtent:
        return ThreadPoolType::READ;
    default:
        return ThreadPoolType::WRITE;
    }
}
}   // namespace copyset
}   // namespace metaserver
}   // namespace curvefs

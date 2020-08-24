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
#include <vector>
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
    InitThreadPool(ThreadPoolType::READ, rconcurrentsize_, rqueuedepth_);
    InitThreadPool(ThreadPoolType::WRITE, wconcurrentsize_, wqueuedepth_);

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
    ThreadPoolType type, int concorrent, int depth) {
    for (int i = 0; i < concorrent; i++) {
        auto asyncth = new (std::nothrow) taskthread(depth);
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

    for (int i = 0; i < concorrent; i++) {
        switch (type) {
        case ThreadPoolType::READ:
            rapplyMap_[i]->th = std::move(
                std::thread(&ConcurrentApplyModule::Run, this, type, i));
            break;

        case ThreadPoolType::WRITE:
            wapplyMap_[i]->th =
                std::thread(&ConcurrentApplyModule::Run, this, type, i);
            break;
        }
    }
}

void ConcurrentApplyModule::Run(ThreadPoolType type, int index) {
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

void ConcurrentApplyModule::Stop() {
    LOG(INFO) << "stop ConcurrentApplyModule...";
    start_ = false;
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
    CountDownEvent event(wconcurrentsize_);
    auto flushtask = [&event]() {
        event.Signal();
    };

    for (int i = 0; i < wconcurrentsize_; i++) {
        wapplyMap_[i]->tq.Push(flushtask);
    }

    event.Wait();
}

ThreadPoolType ConcurrentApplyModule::Schedule(CHUNK_OP_TYPE optype) {
    switch (optype) {
    case CHUNK_OP_READ:
    case CHUNK_OP_RECOVER:
        return ThreadPoolType::READ;
    default:
        return ThreadPoolType::WRITE;
    }
}
}   // namespace concurrent
}   // namespace chunkserver
}   // namespace curve

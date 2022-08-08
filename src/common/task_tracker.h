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
 * Created Date: 2020-07-01
 * Author: xuchaojie
 */

#ifndef SRC_COMMON_TASK_TRACKER_H_
#define SRC_COMMON_TASK_TRACKER_H_

#include <vector>
#include <memory>
#include <list>

#include "src/common/concurrent/concurrent.h"

using ::curve::common::Mutex;
using ::curve::common::ConditionVariable;

namespace curve {
namespace common {

class TaskTracker : public std::enable_shared_from_this<TaskTracker> {
 public:
    TaskTracker()
    : concurrent_(0),
      lastErr_(0) {}

    /**
     * @brief add a trace
     */
    void AddOneTrace();

    /**
     * @brief get task num
     *
     * @return task num
     */
    uint32_t GetTaskNum() const {
        return concurrent_;
    }

    /**
     * @brief Handle task return code
     *
     * @param retCode  return code
     */
    void HandleResponse(int retCode);

    /**
     * @brief wait all task to be done
     */
    void Wait();

    /**
     * @brief wait specific num of task to be done
     *
     * @param num  the specific num of task to be done
     */
    void WaitSome(uint32_t num);

    /**
     * @brief get result
     *
     * @return int  return code
     */
    int GetResult() {
        return lastErr_;
    }

 private:
    // ConditionVariable use to wait task to be done
    ConditionVariable cv_;
    Mutex cv_m;
    // current inflight task num
    std::atomic<uint32_t> concurrent_;
    // error code
    int lastErr_;
};

template <typename CTX>
class ContextTaskTracker : public TaskTracker {
 public:
     void PushResultContext(const CTX &ctx);
     std::list<CTX> PopResultContexts();

 private:
     Mutex ctxMutex_;
     std::list<CTX> contexts_;
};

template <typename CTX>
void ContextTaskTracker<CTX>::PushResultContext(const CTX &ctx) {
    std::unique_lock<Mutex> lk(ctxMutex_);
    contexts_.push_back(ctx);
}

template <typename CTX>
std::list<CTX>
    ContextTaskTracker<CTX>::PopResultContexts() {
    std::unique_lock<Mutex> lk(ctxMutex_);
    std::list<CTX> ret;
    ret.swap(contexts_);
    return ret;
}

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_TASK_TRACKER_H_

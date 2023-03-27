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
 * Project: Curve
 * Created Date: 2023-03-09
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_MESSAGE_QUEUE_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_MESSAGE_QUEUE_H_

#include <glog/logging.h>

#include <string>
#include <atomic>
#include <thread>

#include "src/common/concurrent/task_queue.h"
#include "curvefs/src/common/threading.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curve::common::TaskQueue;
using ::curvefs::common::SetThreadName;

template <typename MessageT>
class MessageQueue {
 public:
    using MessageHandler = std::function<void(const MessageT& message)>;

 public:
    MessageQueue(const std::string& name, size_t bufferSize)
        : name_(name),
          running_(false),
          thread_(),
          handler_(),
          queue_(bufferSize) {}

    void Start() {
        if (running_.exchange(true)) {
            return;
        }

        thread_ = std::thread(&MessageQueue::Consumer, this);
        LOG(INFO) << "MessageQueue [ " << name_ << " ] "
                  << "consumer thread start success";
    }

    void Stop() {
        if (!running_.exchange(false)) {
            return;
        }

        auto wakeup = []() {};
        queue_.Push(wakeup);

        LOG(INFO) << "MessageQueue [ " << name_ << " ] "
                  << "consumer thread stoping...";

        thread_.join();

        LOG(INFO) << "MessageQueue [ " << name_ << " ] "
                  << "consumer thread stopped";
    }

    void Publish(MessageT message) {
        if (handler_ != nullptr) {
            queue_.Push([this, message](){
                this->handler_(message);
            });
        }
    }

    void Subscribe(MessageHandler handler) {
        handler_ = handler;
    }

 private:
    void Consumer() {
        SetThreadName(name_.c_str());
        while (running_.load(std::memory_order_relaxed)) {
            queue_.Pop()();
        }
    }

 private:
    std::string name_;
    std::atomic<bool> running_;
    std::thread thread_;
    MessageHandler handler_;
    TaskQueue queue_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_MESSAGE_QUEUE_H_

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

#include "curvefs/src/client/filesystem/core.h"

namespace curvefs {
namespace client {
namespace filesystem {

template <typename MessageT>
class Channel {
 public:
    explicit Channel(size_t bufferSize)
        : bufferSize_(bufferSize),
          mutex_(),
          inCond_(),
          outCond_(),
          queue_() {}

    void Send(MessageT message) {
        {
            UniqueLock lk(mutex_);
            while (queue_.size() >= capacity_) {
                outCond_.wait(lk);
            }
            queue_->push(std::move(message));
        }
        inCond_.notify_one();
    }

    MessageT Receive() {
        UniqueLock lk(mutex_);
        while (queue_.empty()) {
            inCond_.wait(lk);
        }

        MessageT message = std::move(queue_.front())
        queue_.pop();
        outCond_.notify_one();
        return message;
    }

 private:
    size_t bufferSize_;
    Mutex mutex_;
    std::condition_variable inCond_;
    std::condition_variable outCond_;
    std::queue<MessageT> queue_;
};

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
          channel_(std::make_shared<Channel<MessageT>>(bufferSize)) {}

    bool Run() {
        if (!running_.exchange(true)) {
            thread_ = std::thread(&Consumer, this);
            LOG(INFO) << "MessageQueue [ " << name << " ] "
                      << "consumer thread start success";
        }
    }

    bool Stop() {
        if (running_.exchange(false)) {
            LOG(INFO) << "MessageQueue [ " << name << " ] "
                      << "consumer thread stopping...";
            thread_.join();
            LOG(INFO) << "MessageQueue [ " << name << " ] "
                      << "consumer thread stopped";
        }
        return true;
    }

    void Pub(MessageT message) {
        channel_->Send(message);
    }

    void Sub(MessageHandler handler) {
        handler_ = handler;
    }

 privete:
    void Consumer() {
        for ( ;; ) {
            MessageT message = channel_->Receive();
            if (handler_ != NULL) {
                handler_(message);
            }
        }
    }

 private:
    std::string name_;
    std::atomic<bool> running_;
    std::thread thread_;
    MessageHandler handler_;
    std::shared_ptr<Channel<MessageT>> channel_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_MESSAGE_QUEUE_H_

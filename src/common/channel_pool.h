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
 * Created Date: 2020-01-06
 * Author: charisu
 */

#ifndef SRC_COMMON_CHANNEL_POOL_H_
#define SRC_COMMON_CHANNEL_POOL_H_

#include <brpc/channel.h>
#include <unordered_map>
#include <string>
#include <memory>
#include <utility>
#include <atomic>

#include "src/common/concurrent/concurrent.h"

using ChannelPtr = std::shared_ptr<brpc::Channel>;

namespace curve {
namespace common {

class ChannelPool {
 public:
    ChannelPool() :
        userCount_(0) {}

    ~ChannelPool() = default;

    /**
     * @brief 从channelMap获取或创建并Init到指定地址的channel
     *
     * @param addr 对端的地址
     * @param[out] channelPtr 到指定地址的channel
     *
     * @return 成功返回0，失败返回-1
     */
    int GetOrInitChannel(const std::string& addr,
                         ChannelPtr* channelPtr);

    void AddUser() {
        userCount_.fetch_add(1, std::memory_order_release);
    }

    void RemoveUser() {
        if (1 == userCount_.fetch_sub(1, std::memory_order_acquire)) {
            Clear();
        }
    }

    /**
     * @brief 清空map
     */
    void Clear();

 private:
    Mutex mutex_;
    std::unordered_map<std::string, ChannelPtr> channelMap_;
    std::atomic<int> userCount_;
};

}  // namespace common
}  // namespace curve

#endif   // SRC_COMMON_CHANNEL_POOL_H_


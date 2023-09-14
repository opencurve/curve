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

#include "src/common/concurrent/concurrent.h"

using ChannelPtr = std::shared_ptr<brpc::Channel>;

namespace curve {
namespace common {

class ChannelPool {
 public:
    /**
     * @brief Obtain or create a channel from channelMap and Init it to the specified address
     *
     * @param addr The address of the opposite end
     * @param[out] channelPtr to the specified channel address
     *
     * @return returns 0 for success, -1 for failure
     */
    int GetOrInitChannel(const std::string& addr,
                         ChannelPtr* channelPtr);

    /**
     * @brief Clear map
     */
    void Clear();

 private:
    Mutex mutex_;
    std::unordered_map<std::string, ChannelPtr> channelMap_;
};

}  // namespace common
}  // namespace curve

#endif   // SRC_COMMON_CHANNEL_POOL_H_


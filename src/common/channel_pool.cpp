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

#include "src/common/channel_pool.h"

namespace curve {
namespace common {

int ChannelPool::GetOrInitChannel(const std::string& addr,
                                  ChannelPtr* channelPtr) {
    LockGuard guard(mutex_);
    auto iter = channelMap_.find(addr);
    if (iter != channelMap_.end()) {
        *channelPtr = iter->second;
        return 0;
    }

    auto newChannel = std::make_shared<brpc::Channel>();
    if (newChannel->Init(addr.c_str(), NULL) != 0) {
        LOG(ERROR) << "Fail to init channel to " << addr;
        return -1;
    }
    channelMap_.insert(std::pair<std::string, ChannelPtr>(addr, newChannel));
    *channelPtr = newChannel;
    return 0;
}

void ChannelPool::Clear() {
    LockGuard guard(mutex_);
    channelMap_.clear();
}


}  // namespace common
}  // namespace curve

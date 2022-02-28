/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur Sept 2 2021
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_CLIENT_RPCCLIENT_CHANNEL_MANAGER_H_
#define CURVEFS_SRC_CLIENT_RPCCLIENT_CHANNEL_MANAGER_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <unordered_map>
#include <memory>
#include <list>
#include <string>

#include "src/common/concurrent/rw_lock.h"

namespace curvefs {
namespace client {
namespace rpcclient {
template <typename T> class ChannelManager {
 public:
    using ChannelPtr = std::shared_ptr<brpc::Channel>;
    ChannelPtr GetOrCreateChannel(const T &id,
                                  const butil::EndPoint &leaderAddr);
    void ResetSenderIfNotHealth(const T &csId);

 private:
    curve::common::BthreadRWLock rwlock_;
    std::unordered_map<T, ChannelPtr> channelPool_;
};

template <typename T>
typename ChannelManager<T>::ChannelPtr
ChannelManager<T>::GetOrCreateChannel(const T &id,
                                      const butil::EndPoint &leaderAddr) {
    {
        curve::common::ReadLockGuard guard(rwlock_);
        auto iter = channelPool_.find(id);
        if (channelPool_.end() != iter) {
            return iter->second;
        }
    }

    curve::common::WriteLockGuard guard(rwlock_);
    auto iter = channelPool_.find(id);
    if (channelPool_.end() != iter) {
        return iter->second;
    }

    auto channel = std::make_shared<brpc::Channel>();
    if (0 != channel->Init(leaderAddr, nullptr)) {
        LOG(ERROR) << "failed to init channel to server, " << id << ", "
                   << butil::endpoint2str(leaderAddr).c_str();
        return nullptr;
    } else {
        channelPool_.emplace(id, channel);
        return channel;
    }
}

template <typename T>
void ChannelManager<T>::ResetSenderIfNotHealth(const T &id) {
    curve::common::WriteLockGuard guard(rwlock_);
    auto iter = channelPool_.find(id);

    if (iter == channelPool_.end()) {
        return;
    }

    // check health
    if (0 == iter->second->CheckHealth()) {
        return;
    }

    channelPool_.erase(iter);
}
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_CHANNEL_MANAGER_H_

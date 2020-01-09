/*
 * Project: curve
 * Created Date: 2020-01-06
 * Author: charisu
 * Copyright (c) 2018 netease
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

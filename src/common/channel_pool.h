/*
 * Project: curve
 * Created Date: 2020-01-06
 * Author: charisu
 * Copyright (c) 2018 netease
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
     * @brief 从channelMap获取或创建并Init到指定地址的channel
     *
     * @param addr 对端的地址
     * @param[out] channelPtr 到指定地址的channel
     *
     * @return 成功返回0，失败返回-1
     */
    int GetOrInitChannel(const std::string& addr,
                         ChannelPtr* channelPtr);

    /**
     * @brief 清空map
     */
    void Clear();

 private:
    Mutex mutex_;
    std::unordered_map<std::string, ChannelPtr> channelMap_;
};

}  // namespace common
}  // namespace curve

#endif   // SRC_COMMON_CHANNEL_POOL_H_


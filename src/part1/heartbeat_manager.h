/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART1_HEARTBEAT_MANAGER_H_
#define SRC_PART1_HEARTBEAT_MANAGER_H_

#include <brpc/channel.h>

#include <thread>   // NOLINT
#include <memory>
#include <string>

#include "src/part1/nebd_common.h"
#include "src/part1/nebd_metacache.h"
#include "src/common/interrupt_sleep.h"

namespace nebd {
namespace client {

// Heartbeat 管理类
// 定期向nebd-server发送已打开文件的心跳信息
class HeartbeatManager {
 public:
    explicit HeartbeatManager(std::shared_ptr<NebdClientMetaCache> metaCache);

    ~HeartbeatManager() = default;

    /**
     * @brief: 启动心跳线程
     */
    void Run();

    /**
     * @brief: 停止心跳线程
     */
    void Stop();

    /**
     * @brief 初始化
     * @param heartbeatOption heartbeat 配置项
     * @return 0 初始化成功 / -1 初始化失败
     */
    int Init(const HeartbeatOption& option);

 private:
    /**
     * @brief: 心跳线程执行函数，定期发送心跳消息
     */
    void HeartBetaThreadFunc();

    /**
     * @brief: 向part2发送心跳消息，包括当前已打开的卷信息
     */
    void SendHeartBeat();

 private:
    brpc::Channel channel_;

    HeartbeatOption heartbeatOption_;

    std::shared_ptr<NebdClientMetaCache>  metaCache_;

    std::thread heartbeatThread_;
    common::InterruptibleSleeper sleeper_;

    std::atomic<bool> running_;
    std::atomic<uint64_t> logId_;
};

}  // namespace client
}  // namespace nebd

#endif  // SRC_PART1_HEARTBEAT_MANAGER_H_

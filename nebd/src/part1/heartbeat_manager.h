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

/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 */

#ifndef NEBD_SRC_PART1_HEARTBEAT_MANAGER_H_
#define NEBD_SRC_PART1_HEARTBEAT_MANAGER_H_

#include <brpc/channel.h>

#include <thread>   // NOLINT
#include <memory>
#include <string>

#include "nebd/src/part1/nebd_common.h"
#include "nebd/src/part1/nebd_metacache.h"
#include "nebd/src/common/interrupt_sleep.h"

namespace nebd {
namespace client {

//Heartbeat Management Class
//Regularly send heartbeat information of opened files to nebd-server
class HeartbeatManager {
 public:
    explicit HeartbeatManager(std::shared_ptr<NebdClientMetaCache> metaCache);

    ~HeartbeatManager() {
       Stop();
    }

    /**
     * @brief: Start heartbeat thread
     */
    void Run();

    /**
     * @brief: Stop heartbeat thread
     */
    void Stop();

    /**
     * @brief initialization
     * @param heartbeatOption heartbeat configuration item
     * @return 0 initialization successful/-1 initialization failed
     */
    int Init(const HeartbeatOption& option);

 private:
    /**
     * @brief: Heartbeat thread execution function, sending heartbeat messages regularly
     */
    void HeartBetaThreadFunc();

    /**
     * @brief: Send a heartbeat message to part2, including information about the currently opened volume
     */
    void SendHeartBeat();

 private:
    brpc::Channel channel_;

    HeartbeatOption heartbeatOption_;

    std::shared_ptr<NebdClientMetaCache>  metaCache_;

    std::thread heartbeatThread_;
    nebd::common::InterruptibleSleeper sleeper_;

    std::atomic<bool> running_;
    std::atomic<uint64_t> logId_;
    // nebd version
    std::string nebdVersion_;
    // process id
    int pid_;
};

}  // namespace client
}  // namespace nebd

#endif  // NEBD_SRC_PART1_HEARTBEAT_MANAGER_H_

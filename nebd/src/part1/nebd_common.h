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
 * Project: nebd
 * File Created: 2020-02-07
 * Author: wuhanqing
 */

#ifndef NEBD_SRC_PART1_NEBD_COMMON_H_
#define NEBD_SRC_PART1_NEBD_COMMON_H_

#include <string>

// rpc request configuration item
struct RequestOption {
    // Maximum number of retries for synchronous rpc
    int64_t syncRpcMaxRetryTimes;
    // The retry interval for rpc requests
    int64_t rpcRetryIntervalUs;
    // Maximum retry interval for rpc requests
    int64_t rpcRetryMaxIntervalUs;
    // The retry time in the case of rpc hostdown
    int64_t rpcHostDownRetryIntervalUs;
    // Health check cycle time for brpc
    int64_t rpcHealthCheckIntervalS;
    // The maximum time interval between RPC failure and health check in BRPC
    int64_t rpcMaxDelayHealthCheckIntervalMs;
    // Number of RPC send execution queues
    uint32_t rpcSendExecQueueNum = 2;
};

// Log Configuration Item
struct LogOption {
    // Log storage directory
    std::string logPath;
};

// nebd client configuration item
struct NebdClientOption {
    // part2 socket file address
    std::string serverAddress;
    // File lock path
    std::string fileLockPath;
    // rpc request configuration item
    RequestOption requestOption;
    // Log Configuration Item
    LogOption logOption;
};

// heartbeat configuration item
struct HeartbeatOption {
    // part2 socket file address
    std::string serverAddress;
    // heartbeat interval
    int64_t intervalS;
    // heartbeat RPC timeout
    int64_t rpcTimeoutMs;
};

constexpr uint32_t kDefaultBlockSize = 4096;

#endif  // NEBD_SRC_PART1_NEBD_COMMON_H_

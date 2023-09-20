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

// rpc request配置项
struct RequestOption {
    // 同步rpc的最大重试次数
    int64_t syncRpcMaxRetryTimes;
    // rpc请求的重试间隔
    int64_t rpcRetryIntervalUs;
    // rpc请求的最大重试间隔
    int64_t rpcRetryMaxIntervalUs;
    // rpc hostdown情况下的重试时间
    int64_t rpcHostDownRetryIntervalUs;
    // brpc的健康检查周期时间
    int64_t rpcHealthCheckIntervalS;
    // brpc从rpc失败到进行健康检查的最大时间间隔
    int64_t rpcMaxDelayHealthCheckIntervalMs;
    // rpc发送执行队列个数
    uint32_t rpcSendExecQueueNum = 2;
};

// 日志配置项
struct LogOption {
    // 日志存放目录
    std::string logPath;
};

// nebd client配置项
struct NebdClientOption {
    // part2 socket file address
    std::string serverAddress;
    // 文件锁路径
    std::string fileLockPath;
    // rpc request配置项
    RequestOption requestOption;
    // 日志配置项
    LogOption logOption;
};

// heartbeat配置项
struct HeartbeatOption {
    // part2 socket file address
    std::string serverAddress;
    // heartbeat间隔
    int64_t intervalS;
    // heartbeat rpc超时时间
    int64_t rpcTimeoutMs;
};

constexpr uint32_t kDefaultBlockSize = 4096;

#endif  // NEBD_SRC_PART1_NEBD_COMMON_H_

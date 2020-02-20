/*
 * Project: nebd
 * File Created: 2020-02-07
 * Author: wuhanqing
 * Copyright (c) 2020 NetEase
 */

#ifndef SRC_PART1_NEBD_COMMON_H_
#define SRC_PART1_NEBD_COMMON_H_

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

#endif  // SRC_PART1_NEBD_COMMON_H_

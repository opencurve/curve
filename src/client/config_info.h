/*
 * Project: curve
 * File Created: Saturday, 29th December 2018 3:50:45 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_CONFIG_INFO_H_
#define SRC_CLIENT_CONFIG_INFO_H_

#include <stdint.h>
#include <string>
#include <vector>

/**
 * log的基本配置信息
 * @loglevel: 是log打印等级
 * @path: log打印位置
 */
typedef struct LogInfo {
    int         loglevel;
    std::string logpath;
    LogInfo() {
        loglevel = 2;
    }
} LogInfo_t;

/**
 * in flight IO控制信息
 */
typedef struct InFlightIOCntlInfo {
    uint64_t    maxInFlightRPCNum;
    InFlightIOCntlInfo() {
        maxInFlightRPCNum = 2048;
    }
} InFlightIOCntlInfo_t;

/**
 * mds client的基本配置
 * @rpcTimeoutMs: 设置rpc超时时间
 * @rpcRetryTimes: 设置rpc重试次数
 * @retryIntervalUs: 设置rpc重试间隔时间
 * @synchronizeRPCTimeoutMS: 设置同步调用RPC超时时间
 * @synchronizeRPCRetryTime: 设置同步调用RPC超时重试次数
 * @metaaddrvec: mds server地址
 */
typedef struct MetaServerOption {
    uint64_t  rpcTimeoutMs;
    uint32_t  rpcRetryTimes;
    uint32_t  retryIntervalUs;
    uint32_t  synchronizeRPCTimeoutMS;
    uint32_t  synchronizeRPCRetryTime;
    std::vector<std::string> metaaddrvec;
    MetaServerOption() {
        rpcTimeoutMs = 500;
        rpcRetryTimes = 5;
        retryIntervalUs = 50000;
        synchronizeRPCRetryTime = 3;
        synchronizeRPCTimeoutMS = 500;
    }
} MetaServerOption_t;

/**
 * 租约基本配置
 * @refreshTimesPerLease: 一个租约内续约次数
 */
typedef struct LeaseOption {
    uint32_t    refreshTimesPerLease;
    LeaseOption() {
        refreshTimesPerLease = 5;
    }
} LeaseOption_t;

/**
 * 发送失败的chunk request处理配置
 * @opRetryIntervalUs: 相隔多久再重试
 * @opMaxRetry: 最大重试次数
 */
typedef struct FailureRequestOption {
    uint32_t opRetryIntervalUs;
    uint32_t opMaxRetry;
    FailureRequestOption() {
        opRetryIntervalUs = 200;
        opMaxRetry = 3;
    }
} FailureRequestOption_t;

/**
 * 发送rpc给chunkserver的配置
 * @rpcTimeoutMs: rpc超时时间
 * @rpcRetryTimes: rpc重试次数
 * @enableAppliedIndexRead: 是否开启使用appliedindex read
 */
typedef struct IOSenderOption {
    uint64_t  rpcTimeoutMs;
    uint32_t  rpcRetryTimes;
    bool enableAppliedIndexRead;
    InFlightIOCntlInfo_t inflightOpt;
    FailureRequestOption_t failRequestOpt;
    IOSenderOption() {
        rpcTimeoutMs = 500;
        rpcRetryTimes = 3;
    }
} IOSenderOption_t;

/**
 * scheduler模块基本配置信息
 * @queueCapacity: schedule模块配置的队列深度
 * @threadpoolSize: schedule模块线程数
 */
typedef struct RequestScheduleOption {
    uint32_t queueCapacity;
    uint32_t threadpoolSize;
    IOSenderOption_t ioSenderOpt;
    RequestScheduleOption() {
        queueCapacity = 1024;
        threadpoolSize = 2;
    }
} RequestScheduleOption_t;

/**
 * metaccache模块配置信息
 * @maxUnStableDurationMs: 一个chunkserver在metacache的unstable map中待的最长时间
 * @getLeaderRetry: 获取leader重试次数
 * @retryIntervalUs: 相隔多久进行重试
 */
typedef struct MetaCacheOption {
    uint32_t maxUnStableDurationMs;
    uint32_t getLeaderRetry;
    uint32_t retryIntervalUs;
    uint32_t getLeaderTimeOutMs;
    MetaCacheOption() {
        getLeaderRetry = 3;
        retryIntervalUs = 500;
        getLeaderTimeOutMs = 1000;
        maxUnStableDurationMs = 30000;  // 30s
    }
} MetaCacheOption_t;

/**
 * IO 拆分模块配置信息
 * @ioSplitMaxSizeKB: 拆分后一个request的最大大小
 */
typedef struct IOSplitOPtion {
    uint64_t  ioSplitMaxSizeKB;
    IOSplitOPtion() {
        ioSplitMaxSizeKB = 64;
    }
} IOSplitOPtion_t;

/**
 * 任务队列配置信息
 */
typedef struct TaskThreadOption {
    uint64_t    taskQueueCapacity;
    uint32_t    taskThreadPoolSize;
    TaskThreadOption() {
        taskQueueCapacity = 500000;
        taskThreadPoolSize = 1;
    }
} TaskThreadOption_t;

/**
 * IOOption存储了当前io 操作所需要的所有配置信息
 */
typedef struct IOOption {
    IOSplitOPtion_t         ioSplitOpt;
    IOSenderOption_t        ioSenderOpt;
    MetaCacheOption_t       metaCacheOpt;
    TaskThreadOption_t      taskThreadOpt;
    RequestScheduleOption_t reqSchdulerOpt;
} IOOption_t;

/**
 * client一侧常规的共同的配置信息
 */
typedef struct CommonConfigOpt {
    bool    registerToMDS;
    CommonConfigOpt() {
        registerToMDS = false;
    }
} CommonConfigOpt_t;

/**
 * ClientConfigOption是外围快照系统需要设置的配置信息
 */
typedef struct ClientConfigOption {
    LogInfo_t                  loginfo;
    IOOption_t                 ioOpt;
    CommonConfigOpt_t          commonOpt;
    MetaServerOption_t         metaServerOpt;
} ClientConfigOption_t;

/**
 * FileServiceOption是QEMU侧总体配置信息
 */
typedef struct FileServiceOption {
    LogInfo_t                 loginfo;
    IOOption_t                ioOpt;
    LeaseOption_t             leaseOpt;
    CommonConfigOpt_t         commonOpt;
    MetaServerOption_t        metaServerOpt;
} FileServiceOption_t;

#endif  // SRC_CLIENT_CONFIG_INFO_H_

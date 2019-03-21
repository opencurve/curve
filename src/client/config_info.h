/*
 * Project: curve
 * File Created: Saturday, 29th December 2018 3:50:45 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVE_CONFIG_INFO_H
#define CURVE_CONFIG_INFO_H

#include <stdint.h>
#include <string>
#include <vector>

/**
 * log的基本配置信息
 * @loglevel: 是log打印等级
 */
typedef struct LogInfo {
    int     loglevel;
    LogInfo() {
        loglevel = 2;
    }
} LogInfo_t;

/**
 * mds client的基本配置
 * @rpc_timeout_ms: 设置rpc超时时间
 * @rpc_retry_times: 设置rpc重试次数
 * @metaaddrvec: mds server地址
 */
typedef struct MetaServerOption {
    uint64_t  rpc_timeout_ms;
    uint16_t  rpc_retry_times;
    std::vector<std::string> metaaddrvec;
    MetaServerOption() {
        rpc_timeout_ms = 500;
        rpc_retry_times = 3;
    }
} MetaServerOption_t;

/**
 * 租约基本配置
 * @refresh_times_perLease: 一个租约内续约次数
 */
typedef struct LeaseOption {
    uint16_t    refresh_times_perLease;
    LeaseOption() {
        refresh_times_perLease = 5;
    }
} LeaseOption_t;

/**
 * 发送失败的chunk request处理配置
 * @client_chunk_op_retry_interval_us: 相隔多久再重试
 * @client_chunk_op_max_retry: 最大重试次数
 */
typedef struct FailureRequestOption {
    uint32_t client_chunk_op_retry_interval_us;
    uint32_t client_chunk_op_max_retry;
    FailureRequestOption() {
        client_chunk_op_retry_interval_us = 200;
        client_chunk_op_max_retry = 3;
    }
} FailureRequestOption_t;

/**
 * 发送rpc给chunkserver的配置
 * @rpc_timeout_ms: rpc超时时间
 * @rpc_retry_times: rpc重试次数
 * @enable_applied_index_read: 是否开启使用appliedindex read
 */
typedef struct IOSenderOption {
    uint64_t  rpc_timeout_ms;
    uint16_t  rpc_retry_times;
    bool enable_applied_index_read;
    FailureRequestOption_t failreqopt;
    IOSenderOption() {
        rpc_timeout_ms = 500;
        rpc_retry_times = 3;
    }
} IOSenderOption_t;

/**
 * scheduler模块基本配置信息
 * @request_scheduler_queue_capacity: schedule模块配置的队列深度
 * @request_scheduler_threadpool_size: schedule模块线程数
 */
typedef struct RequestScheduleOption {
    uint32_t request_scheduler_queue_capacity;
    uint32_t request_scheduler_threadpool_size;
    IOSenderOption_t iosenderopt;
    RequestScheduleOption() {
        request_scheduler_queue_capacity = 1024;
        request_scheduler_threadpool_size = 2;
    }
} RequestScheduleOption_t;

/**
 * metaccache模块配置信息
 * @get_leader_retry: 获取leader重试次数
 * @get_leader_retry_interval_us: 相隔多久进行重试
 */
typedef struct MetaCacheOption {
    uint32_t get_leader_retry;
    uint32_t get_leader_retry_interval_us;
    MetaCacheOption() {
        get_leader_retry = 3;
        get_leader_retry_interval_us = 200;
    }
} MetaCacheOption_t;

/**
 * IO 拆分模块配置信息
 * @io_split_max_size_kb: 拆分后一个request的最大大小
 */
typedef struct IOSplitOPtion {
    uint64_t  io_split_max_size_kb;
    IOSplitOPtion() {
        io_split_max_size_kb = 64;
    }
} IOSplitOPtion_t;

/**
 * IOOption存储了当前io 操作所需要的所有配置信息
 */
typedef struct IOOption {
    IOSplitOPtion_t  iosplitopt;
    IOSenderOption_t iosenderopt;
    MetaCacheOption_t   metacacheopt;
    RequestScheduleOption_t reqschopt;
} IOOption_t;

/**
 * ClientConfigOption是外围快照系统需要设置的配置信息
 */
typedef struct ClientConfigOption {
    LogInfo_t                  loginfo;
    IOOption_t                 ioopt;
    MetaServerOption_t         metaserveropt;
} ClientConfigOption_t;

/**
 * FileServiceOption是QEMU侧总体配置信息
 */
typedef struct FileServiceOption {
    LogInfo_t                 loginfo;
    IOOption_t                ioopt;
    LeaseOption_t             leaseopt;
    MetaServerOption_t        metaserveropt;
} FileServiceOption_t;

#endif  // ! CURVE_CONFIG_INFO_H

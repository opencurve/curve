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

typedef struct LogInfo {
    int     loglevel;
} LogInfo_t;

typedef struct MetaServerOption {
    uint64_t  rpc_timeout_ms;
    uint16_t  rpc_retry_times;
    std::vector<std::string> metaaddrvec;
} MetaServerOption_t;

typedef struct LeaseOption {
    uint16_t    refreshTimesPerLease;
} LeaseOption_t;

typedef struct FailureRequestOption {
    uint32_t client_chunk_op_retry_interval_us;
    uint32_t client_chunk_op_max_retry;
} FailureRequestOption_t;

typedef struct IOSenderOption {
    uint64_t  rpc_timeout_ms;
    uint16_t  rpc_retry_times;
    bool enable_applied_index_read;
    FailureRequestOption_t failreqopt;
} IOSenderOption_t;

typedef struct RequestScheduleOption {
    uint32_t request_scheduler_queue_capacity;
    uint32_t request_scheduler_threadpool_size;
    IOSenderOption_t iosenderopt;
} RequestScheduleOption_t;

typedef struct MetaCacheOption {
    uint32_t get_leader_retry;
    uint32_t get_leader_retry_interval_us;
} MetaCacheOption_t;

typedef struct IOSplitOPtion {
    uint64_t  io_split_max_size_kb;
} IOSplitOPtion_t;

typedef struct IOOption {
    IOSplitOPtion_t  iosplitopt;
    IOSenderOption_t iosenderopt;
    MetaCacheOption_t   metacacheopt;
    RequestScheduleOption_t reqschopt;
} IOOption_t;

typedef struct ClientConfigOption {
     int                        loglevel;
     IOOption_t                 ioopt;
     MetaServerOption_t         metaserveropt;
} ClientConfigOption_t;

typedef struct FileServiceOption {
    int                       loglevel;
    IOOption_t                ioopt;
    LeaseOption_t             leaseopt;
    MetaServerOption_t        metaserveropt;
} FileServiceOption_t;

#endif  // ! CURVE_CONFIG_INFO_H

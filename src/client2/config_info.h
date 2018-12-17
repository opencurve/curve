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

typedef struct LogInfo {
    int     loglevel;
} LogInfo_t;

typedef struct MetaServerOption {
    std::string metaaddr;
} MetaServerOption_t;

typedef struct RequestScheduleOption {
    uint32_t request_scheduler_queue_capacity;
    uint32_t request_scheduler_threadpool_size;
} RequestScheduleOption_t;

typedef struct FailureRequestOption {
    uint32_t client_chunk_op_retry_interval_us;
    uint32_t client_chunk_op_max_retry;
} FailureRequestOption_t;

typedef struct ContextSlabOption {
    uint32_t pre_allocate_context_num;
} ContextSlabOption_t;

typedef struct MetaCacheOption {
    uint32_t get_leader_retry;
    uint32_t get_leader_retry_interval_us;
} MetaCacheOption_t;

typedef struct IOOption {
    bool enable_applied_index_read;
    uint64_t  io_split_max_size_kb;
} IOOption_t;

typedef struct ClientConfigOption {
     int                        loglevel;
     IOOption_t                 ioopt;
     MetaCacheOption_t          metacacheopt;
     MetaServerOption_t         metaserveropt;
     ContextSlabOption_t        ctxslabopt;
     FailureRequestOption_t     failreqslab;
     RequestScheduleOption_t    reqslabopt;
} ClientConfigOption_t;

#endif  // ! CURVE_CONFIG_INFO_H

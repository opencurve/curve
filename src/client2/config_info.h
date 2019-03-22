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
    uint32_t queueCapacity;
    uint32_t threadpoolSize;
} RequestScheduleOption_t;

typedef struct FailureRequestOption {
    uint32_t opRetryIntervalUs;
    uint32_t opMaxRetry;
} FailureRequestOption_t;

typedef struct ContextSlabOption {
    uint32_t pre_allocate_context_num;
} ContextSlabOption_t;

typedef struct MetaCacheOption {
    uint32_t getLeaderRetry;
    uint32_t retryIntervalUs;
} MetaCacheOption_t;

typedef struct IOOption {
    bool enableAppliedIndexRead;
    uint64_t  ioSplitMaxSize;
} IOOption_t;

typedef struct ClientConfigOption {
     int                        loglevel;
     IOOption_t                 ioOpt;
     MetaCacheOption_t          metaCacheOpt;
     MetaServerOption_t         metaServerOpt;
     ContextSlabOption_t        ctxslabopt;
     FailureRequestOption_t     failreqslab;
     RequestScheduleOption_t    reqslabopt;
} ClientConfigOption_t;

#endif  // ! CURVE_CONFIG_INFO_H

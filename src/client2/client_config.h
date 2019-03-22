/*
 * Project: curve
 * File Created: Tuesday, 23rd October 2018 4:46:29 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_CLIENT_CONFIG_H
#define CURVE_CLIENT_CONFIG_H

#include <glog/logging.h>
#include <string>
#include "src/common/configuration.h"
#include "src/client2/config_info.h"

namespace curve {
namespace client {

class ClientConfig {
 public:
    static int Init(const char* configpath) {
        conf_.SetConfigPath(configpath);
        if (!conf_.LoadConfig()) {
            LOG(ERROR) << "Load config failed, config path = " << configpath;
            return -1;
        }

        metaserveroption_.metaaddr
        = conf_.GetStringValue("metaserver_addr");

        contextslaboption_.pre_allocate_context_num
        = conf_.GetIntValue("pre_allocate_context_num", 2048);

        requestscheduleoption_.queueCapacity
        = conf_.GetIntValue("queueCapacity", 4096);
        requestscheduleoption_.threadpoolSize
        = conf_.GetIntValue("threadpoolSize", 2);

        failrequestoptioon_.opMaxRetry
        = conf_.GetIntValue("opMaxRetry", 3);
        failrequestoptioon_.opRetryIntervalUs
        = conf_.GetIntValue("opRetryIntervalUs", 500);

        metacacheoption_.getLeaderRetry
        = conf_.GetIntValue("getLeaderRetry", 3);
        metacacheoption_.retryIntervalUs
        = conf_.GetIntValue("retryIntervalUs", 500);

        ioption_.enableAppliedIndexRead
        = conf_.GetIntValue("enableAppliedIndexRead", 1);
        ioption_.ioSplitMaxSize
        = conf_.GetIntValue("ioSplitMaxSize", 64);

        loginfo_.loglevel = conf_.GetIntValue("loglevel", 2);
        return 0;
    }

    static int Init(ClientConfigOption_t opt) {
        metaserveroption_.metaaddr = opt.metaServerOpt.metaaddr;

        contextslaboption_.pre_allocate_context_num
        = opt.ctxslabopt.pre_allocate_context_num;

        requestscheduleoption_.queueCapacity
        = opt.reqslabopt.queueCapacity;
        requestscheduleoption_.threadpoolSize
        = opt.reqslabopt.threadpoolSize;

        failrequestoptioon_.opMaxRetry
        = opt.failreqslab.opMaxRetry;
        failrequestoptioon_.opRetryIntervalUs
        = opt.failreqslab.opRetryIntervalUs;

        metacacheoption_.getLeaderRetry
        = opt.metaCacheOpt.getLeaderRetry;
        metacacheoption_.retryIntervalUs
        = opt.metaCacheOpt.retryIntervalUs;

        ioption_.enableAppliedIndexRead
        = opt.ioOpt.enableAppliedIndexRead;
        ioption_.ioSplitMaxSize
        = opt.ioOpt.ioSplitMaxSize;
        return 0;
    }

    static IOOption_t GetIOOption() {
        return ioption_;
    }

    static LogInfo_t GetLogInfo() {
        return loginfo_;
    }
    static MetaCacheOption_t GetMetaCacheOption() {
        return metacacheoption_;
    }

    static MetaServerOption_t GetMetaServerOption() {
        return metaserveroption_;
    }

    static ContextSlabOption_t GetContextSlabOption() {
        return contextslaboption_;
    }
    static FailureRequestOption_t GetFailureRequestOption() {
        return failrequestoptioon_;
    }

    static RequestScheduleOption_t GetRequestSchedulerOption() {
        return requestscheduleoption_;
    }

 private:
    static LogInfo_t                loginfo_;
    static IOOption_t               ioption_;
    static MetaCacheOption_t        metacacheoption_;
    static MetaServerOption_t       metaserveroption_;
    static ContextSlabOption_t      contextslaboption_;
    static FailureRequestOption_t   failrequestoptioon_;
    static RequestScheduleOption_t  requestscheduleoption_;

    static common::Configuration    conf_;
};
}   // namespace client
}   // namespace curve

#endif  // !CURVE_CLIENT_CONFIG_H

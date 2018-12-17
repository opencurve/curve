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

        requestscheduleoption_.request_scheduler_queue_capacity
        = conf_.GetIntValue("request_scheduler_queue_capacity", 4096);
        requestscheduleoption_.request_scheduler_threadpool_size
        = conf_.GetIntValue("request_scheduler_threadpool_size", 2);

        failrequestoptioon_.client_chunk_op_max_retry
        = conf_.GetIntValue("client_chunk_op_max_retry", 3);
        failrequestoptioon_.client_chunk_op_retry_interval_us
        = conf_.GetIntValue("client_chunk_op_retry_interval_us", 500);

        metacacheoption_.get_leader_retry
        = conf_.GetIntValue("get_leader_retry", 3);
        metacacheoption_.get_leader_retry_interval_us
        = conf_.GetIntValue("get_leader_retry_interval_us", 500);

        ioption_.enable_applied_index_read
        = conf_.GetIntValue("enable_applied_index_read", 1);
        ioption_.io_split_max_size_kb
        = conf_.GetIntValue("io_split_max_size_kb", 64);

        loginfo_.loglevel = conf_.GetIntValue("loglevel", 2);
        return 0;
    }

    static int Init(ClientConfigOption_t opt) {
        metaserveroption_.metaaddr = opt.metaserveropt.metaaddr;

        contextslaboption_.pre_allocate_context_num
        = opt.ctxslabopt.pre_allocate_context_num;

        requestscheduleoption_.request_scheduler_queue_capacity
        = opt.reqslabopt.request_scheduler_queue_capacity;
        requestscheduleoption_.request_scheduler_threadpool_size
        = opt.reqslabopt.request_scheduler_threadpool_size;

        failrequestoptioon_.client_chunk_op_max_retry
        = opt.failreqslab.client_chunk_op_max_retry;
        failrequestoptioon_.client_chunk_op_retry_interval_us
        = opt.failreqslab.client_chunk_op_retry_interval_us;

        metacacheoption_.get_leader_retry
        = opt.metacacheopt.get_leader_retry;
        metacacheoption_.get_leader_retry_interval_us
        = opt.metacacheopt.get_leader_retry_interval_us;

        ioption_.enable_applied_index_read
        = opt.ioopt.enable_applied_index_read;
        ioption_.io_split_max_size_kb
        = opt.ioopt.io_split_max_size_kb;
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

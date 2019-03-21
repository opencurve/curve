/*
 * Project: curve
 * File Created: Tuesday, 23rd October 2018 4:57:56 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include "src/client/client_config.h"

namespace curve {
namespace client {
int ClientConfig::Init(const char* configpath) {
    conf_.SetConfigPath(configpath);
    if (!conf_.LoadConfig()) {
        LOG(ERROR) << "Load config failed, config path = " << configpath;
        return -1;
    }

    fileServiceOption_.loginfo.loglevel = conf_.GetIntValue("loglevel", 2);

    fileServiceOption_.ioopt.iosplitopt.io_split_max_size_kb
    = conf_.GetIntValue("io_split_max_size_kb", 64);

    fileServiceOption_.ioopt.iosenderopt.enable_applied_index_read
    = conf_.GetIntValue("enable_applied_index_read", 1);
    fileServiceOption_.ioopt.iosenderopt.rpc_timeout_ms
    = conf_.GetIntValue("rpc_timeout_ms", 500);
    fileServiceOption_.ioopt.iosenderopt.rpc_retry_times
    = conf_.GetIntValue("rpc_retry_times", 3);

    fileServiceOption_.ioopt.iosenderopt.failreqopt.client_chunk_op_max_retry
    = conf_.GetIntValue("client_chunk_op_max_retry", 3);
    fileServiceOption_.ioopt.iosenderopt.failreqopt.client_chunk_op_retry_interval_us   // NOLINT
    = conf_.GetIntValue("client_chunk_op_retry_interval_us", 500);

    fileServiceOption_.ioopt.metacacheopt.get_leader_retry
    = conf_.GetIntValue("get_leader_retry", 3);
    fileServiceOption_.ioopt.metacacheopt.get_leader_retry_interval_us
    = conf_.GetIntValue("get_leader_retry_interval_us", 500);

    fileServiceOption_.ioopt.reqschopt.request_scheduler_queue_capacity
    = conf_.GetIntValue("request_scheduler_queue_capacity", 4096);
    fileServiceOption_.ioopt.reqschopt.request_scheduler_threadpool_size
    = conf_.GetIntValue("request_scheduler_threadpool_size", 2);
    fileServiceOption_.ioopt.reqschopt.iosenderopt
    = fileServiceOption_.ioopt.iosenderopt;

    fileServiceOption_.leaseopt.refresh_times_perLease
    = conf_.GetIntValue("lease_refresh_times_pertime", 4);

    fileServiceOption_.metaserveropt.metaaddrvec.push_back(conf_.
                                            GetStringValue("metaserver_addr"));
    fileServiceOption_.metaserveropt.rpc_timeout_ms
    = conf_.GetIntValue("rpc_timeout_ms", 500);
    fileServiceOption_.metaserveropt.rpc_retry_times
    = conf_.GetIntValue("rpc_timeout_ms", 500);

    return 0;
}

FileServiceOption_t ClientConfig::GetFileServiceOption() {
    return fileServiceOption_;
}
}   // namespace client
}   // namespace curve

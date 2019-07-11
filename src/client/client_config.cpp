/*
 * Project: curve
 * File Created: Tuesday, 23rd October 2018 4:57:56 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <vector>

#include "src/client/client_config.h"
#include "src/common/string_util.h"
#include "src/common/net_common.h"

namespace curve {
namespace client {
int ClientConfig::Init(const char* configpath) {
    conf_.SetConfigPath(configpath);
    if (!conf_.LoadConfig()) {
        LOG(ERROR) << "Load config failed, config path = " << configpath;
        return -1;
    }

    if (!conf_.GetIntValue("loglevel", &fileServiceOption_.loginfo.loglevel)) {
        LOG(ERROR) << "config no loglevel info";
        return -1;
    }

    if (!conf_.GetStringValue("logpath", &fileServiceOption_.loginfo.logpath)) {
        LOG(ERROR) << "config no logpath info";
        return -1;
    }

    if (!conf_.GetUInt64Value("ioSplitMaxSizeKB",
         &fileServiceOption_.ioOpt.ioSplitOpt.ioSplitMaxSizeKB)) {
        LOG(ERROR) << "config no ioSplitMaxSizeKB info";
        return -1;
    }

    if (!conf_.GetBoolValue("enableAppliedIndexRead",
         &fileServiceOption_.ioOpt.ioSenderOpt.enableAppliedIndexRead)) {
        LOG(ERROR) << "config no enableAppliedIndexRead info";
        return -1;
    }

    if (!conf_.GetUInt64Value("rpcTimeoutMs",
         &fileServiceOption_.ioOpt.ioSenderOpt.rpcTimeoutMs)) {
        LOG(ERROR) << "config no rpcTimeoutMs info";
        return -1;
    }

    if (!conf_.GetUInt32Value("rpcRetryTimes",
         &fileServiceOption_.ioOpt.ioSenderOpt.rpcRetryTimes)) {
        LOG(ERROR) << "config no rpcRetryTimes info";
        return -1;
    }

    if (!conf_.GetUInt32Value("opMaxRetry",
        &fileServiceOption_.ioOpt.ioSenderOpt.failRequestOpt.opMaxRetry)) {
        LOG(ERROR) << "config no opMaxRetry info";
        return -1;
    }

    if (!conf_.GetUInt32Value("opRetryIntervalUs",
        &fileServiceOption_.ioOpt.ioSenderOpt.failRequestOpt.opRetryIntervalUs)) {  //  NOLINT
        LOG(ERROR) << "config no opRetryIntervalUs info";
        return -1;
    }

    if (!conf_.GetUInt64Value("maxInFlightRPCNum",
        &fileServiceOption_.ioOpt.ioSenderOpt.inflightOpt.maxInFlightRPCNum)) {  //  NOLINT
        LOG(ERROR) << "config no maxInFlightRPCNum info";
        return -1;
    }

    if (!conf_.GetUInt32Value("getLeaderRetry",
        &fileServiceOption_.ioOpt.metaCacheOpt.getLeaderRetry)) {
        LOG(ERROR) << "config no getLeaderRetry info";
        return -1;
    }

    if (!conf_.GetUInt32Value("retryIntervalUs",
        &fileServiceOption_.ioOpt.metaCacheOpt.retryIntervalUs)) {
        LOG(ERROR) << "config no retryIntervalUs info";
        return -1;
    }

    if (!conf_.GetUInt32Value("getLeaderTimeOutMs",
        &fileServiceOption_.ioOpt.metaCacheOpt.getLeaderTimeOutMs)) {
        LOG(ERROR) << "config no getLeaderTimeOutMs info";
        return -1;
    }

    if (!conf_.GetUInt32Value("queueCapacity",
        &fileServiceOption_.ioOpt.reqSchdulerOpt.queueCapacity)) {
        LOG(ERROR) << "config no queueCapacity info";
        return -1;
    }

    if (!conf_.GetUInt32Value("threadpoolSize",
        &fileServiceOption_.ioOpt.reqSchdulerOpt.threadpoolSize)) {
        LOG(ERROR) << "config no threadpoolSize info";
        return -1;
    }

    if (!conf_.GetUInt32Value("refreshTimesPerLease",
        &fileServiceOption_.leaseOpt.refreshTimesPerLease)) {
        LOG(ERROR) << "config no refreshTimesPerLease info";
        return -1;
    }

    fileServiceOption_.ioOpt.reqSchdulerOpt.ioSenderOpt
    = fileServiceOption_.ioOpt.ioSenderOpt;


    if (!conf_.GetUInt64Value("taskQueueCapacity",
        &fileServiceOption_.ioOpt.taskThreadOpt.taskQueueCapacity)) {
        LOG(ERROR) << "config no taskQueueCapacity info";
        return -1;
    }

    if (!conf_.GetUInt32Value("taskThreadPoolSize",
        &fileServiceOption_.ioOpt.taskThreadOpt.taskThreadPoolSize)) {
        LOG(ERROR) << "config no taskThreadPoolSize info";
        return -1;
    }

    std::string metaAddr;
    if (!conf_.GetStringValue("metaserver_addr", &metaAddr)) {
        LOG(ERROR) << "config no metaserver_addr info";
        return -1;
    }

    std::vector<std::string> mdsAddr;
    common::SplitString(metaAddr, "@", &mdsAddr);
    fileServiceOption_.metaServerOpt.metaaddrvec.assign(mdsAddr.begin(),
                                                        mdsAddr.end());
    for (auto& addr : fileServiceOption_.metaServerOpt.metaaddrvec) {
        if (!curve::common::NetCommon::CheckAddressValid(addr)) {
            LOG(ERROR) << "address valid!";
            return -1;
        }
    }

    if (!conf_.GetUInt64Value("rpcTimeoutMs",
        &fileServiceOption_.metaServerOpt.rpcTimeoutMs)) {
        LOG(ERROR) << "config no rpcTimeoutMs info";
        return -1;
    }

    if (!conf_.GetUInt32Value("rpcRetryTimes",
        &fileServiceOption_.metaServerOpt.rpcRetryTimes)) {
        LOG(ERROR) << "config no rpcRetryTimes info";
        return -1;
    }

    if (!conf_.GetUInt32Value("retryIntervalUs",
        &fileServiceOption_.metaServerOpt.retryIntervalUs)) {
        LOG(ERROR) << "config no retryIntervalUs info";
        return -1;
    }

    if (!conf_.GetUInt32Value("synchronizeRPCTimeoutMS",
        &fileServiceOption_.metaServerOpt.synchronizeRPCTimeoutMS)) {
        LOG(ERROR) << "config no synchronizeRPCTimeoutMS info";
        return -1;
    }

    if (!conf_.GetUInt32Value("synchronizeRPCRetryTime",
        &fileServiceOption_.metaServerOpt.synchronizeRPCRetryTime)) {
        LOG(ERROR) << "config no synchronizeRPCRetryTime info";
        return -1;
    }

    return 0;
}

FileServiceOption_t ClientConfig::GetFileServiceOption() {
    return fileServiceOption_;
}

uint16_t ClientConfig::GetDummyserverStartPort() {
    return conf_.GetIntValue("dummyServerStartPort", 9000);
}

}   // namespace client
}   // namespace curve

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

#define RETURN_IF_FALSE(x) if (x == false) return -1;

namespace curve {
namespace client {
int ClientConfig::Init(const char* configpath) {
    conf_.SetConfigPath(configpath);
    if (!conf_.LoadConfig()) {
        LOG(ERROR) << "Load config failed, config path = " << configpath;
        return -1;
    }

    bool ret = false;

    ret = conf_.GetIntValue("loglevel", &fileServiceOption_.loginfo.loglevel);
    LOG_IF(ERROR, ret == false) << "config no loglevel info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetStringValue("logpath", &fileServiceOption_.loginfo.logpath);
    LOG_IF(ERROR, ret == false) << "config no logpath info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt64Value("ioSplitMaxSizeKB",
          &fileServiceOption_.ioOpt.ioSplitOpt.ioSplitMaxSizeKB);
    LOG_IF(ERROR, ret == false) << "config no ioSplitMaxSizeKB info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetBoolValue("enableAppliedIndexRead",
          &fileServiceOption_.ioOpt.ioSenderOpt.enableAppliedIndexRead);
    LOG_IF(ERROR, ret == false) << "config no enableAppliedIndexRead info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt64Value("rpcTimeoutMs",
          &fileServiceOption_.ioOpt.ioSenderOpt.rpcTimeoutMs);
    LOG_IF(ERROR, ret == false) << "config no rpcTimeoutMs info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("rpcRetryTimes",
          &fileServiceOption_.ioOpt.ioSenderOpt.rpcRetryTimes);
    LOG_IF(ERROR, ret == false) << "config no rpcRetryTimes info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("opMaxRetry",
          &fileServiceOption_.ioOpt.ioSenderOpt.failRequestOpt.opMaxRetry);
    LOG_IF(ERROR, ret == false) << "config no opMaxRetry info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("opRetryIntervalUs",
        &fileServiceOption_.ioOpt.ioSenderOpt.failRequestOpt.opRetryIntervalUs);
    LOG_IF(ERROR, ret == false) << "config no opRetryIntervalUs info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt64Value("maxInFlightRPCNum",
        &fileServiceOption_.ioOpt.ioSenderOpt.inflightOpt.maxInFlightRPCNum);
    LOG_IF(ERROR, ret == false) << "config no maxInFlightRPCNum info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("getLeaderRetry",
        &fileServiceOption_.ioOpt.metaCacheOpt.getLeaderRetry);
    LOG_IF(ERROR, ret == false) << "config no getLeaderRetry info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("retryIntervalUs",
        &fileServiceOption_.ioOpt.metaCacheOpt.retryIntervalUs);
    LOG_IF(ERROR, ret == false) << "config no retryIntervalUs info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("getLeaderTimeOutMs",
        &fileServiceOption_.ioOpt.metaCacheOpt.getLeaderTimeOutMs);
    LOG_IF(ERROR, ret == false) << "config no getLeaderTimeOutMs info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("queueCapacity",
        &fileServiceOption_.ioOpt.reqSchdulerOpt.queueCapacity);
    LOG_IF(ERROR, ret == false) << "config no queueCapacity info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("threadpoolSize",
        &fileServiceOption_.ioOpt.reqSchdulerOpt.threadpoolSize);
    LOG_IF(ERROR, ret == false) << "config no threadpoolSize info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("refreshTimesPerLease",
        &fileServiceOption_.leaseOpt.refreshTimesPerLease);
    LOG_IF(ERROR, ret == false) << "config no refreshTimesPerLease info";
    RETURN_IF_FALSE(ret)

    fileServiceOption_.ioOpt.reqSchdulerOpt.ioSenderOpt
    = fileServiceOption_.ioOpt.ioSenderOpt;


    ret = conf_.GetUInt64Value("taskQueueCapacity",
        &fileServiceOption_.ioOpt.taskThreadOpt.taskQueueCapacity);
    LOG_IF(ERROR, ret == false) << "config no taskQueueCapacity info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("taskThreadPoolSize",
        &fileServiceOption_.ioOpt.taskThreadOpt.taskThreadPoolSize);
    LOG_IF(ERROR, ret == false) << "config no taskThreadPoolSize info";
    RETURN_IF_FALSE(ret)

    std::string metaAddr;
    ret = conf_.GetStringValue("metaserver_addr", &metaAddr);
    LOG_IF(ERROR, ret == false) << "config no metaserver_addr info";
    RETURN_IF_FALSE(ret)

    std::vector<std::string> mdsAddr;
    common::SplitString(metaAddr, ",", &mdsAddr);
    fileServiceOption_.metaServerOpt.metaaddrvec.assign(mdsAddr.begin(),
                                                        mdsAddr.end());
    for (auto& addr : fileServiceOption_.metaServerOpt.metaaddrvec) {
        if (!curve::common::NetCommon::CheckAddressValid(addr)) {
            LOG(ERROR) << "address valid!";
            return -1;
        }
    }

    ret = conf_.GetUInt64Value("rpcTimeoutMs",
        &fileServiceOption_.metaServerOpt.rpcTimeoutMs);
    LOG_IF(ERROR, ret == false) << "config no rpcTimeoutMs info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("rpcRetryTimes",
        &fileServiceOption_.metaServerOpt.rpcRetryTimes);
    LOG_IF(ERROR, ret == false) << "config no rpcRetryTimes info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("retryIntervalUs",
        &fileServiceOption_.metaServerOpt.retryIntervalUs);
    LOG_IF(ERROR, ret == false) << "config no retryIntervalUs info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("synchronizeRPCTimeoutMS",
        &fileServiceOption_.metaServerOpt.synchronizeRPCTimeoutMS);
    LOG_IF(ERROR, ret == false) << "config no synchronizeRPCTimeoutMS info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("synchronizeRPCRetryTime",
        &fileServiceOption_.metaServerOpt.synchronizeRPCRetryTime);
    LOG_IF(ERROR, ret == false) << "config no synchronizeRPCRetryTime info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetBoolValue("registerToMDS",
        &fileServiceOption_.commonOpt.registerToMDS);
    LOG_IF(ERROR, ret == false) << "config no registerToMDS info";
    RETURN_IF_FALSE(ret)

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

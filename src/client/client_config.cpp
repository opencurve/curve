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

    conf_.ExposeMetric("client_config");

    bool ret = false;

    ret = conf_.GetIntValue("global.logLevel", &fileServiceOption_.loginfo.logLevel);   // NOLINT
    LOG_IF(ERROR, ret == false) << "config no global.logLevel info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetStringValue("global.logPath", &fileServiceOption_.loginfo.logPath);            // NOLINT
    LOG_IF(ERROR, ret == false) << "config no global.logPath info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt64Value("global.fileIOSplitMaxSizeKB",
          &fileServiceOption_.ioOpt.ioSplitOpt.fileIOSplitMaxSizeKB);
    LOG_IF(ERROR, ret == false) << "config no global.fileIOSplitMaxSizeKB info";           // NOLINT
    RETURN_IF_FALSE(ret)

    ret = conf_.GetBoolValue("chunkserver.enableAppliedIndexRead",
          &fileServiceOption_.ioOpt.ioSenderOpt.chunkserverEnableAppliedIndexRead);        // NOLINT
    LOG_IF(ERROR, ret == false) << "config no chunkserver.enableAppliedIndexRead info";     // NOLINT
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("chunkserver.opMaxRetry",
          &fileServiceOption_.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPMaxRetry);    // NOLINT
    LOG_IF(ERROR, ret == false) << "config no chunkserver.opMaxRetry info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt64Value("chunkserver.opRetryIntervalUS",
        &fileServiceOption_.ioOpt.ioSenderOpt.failRequestOpt.chunkserverOPRetryIntervalUS);   // NOLINT
    LOG_IF(ERROR, ret == false) << "config no chunkserver.opRetryIntervalUS info";            // NOLINT
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt64Value("chunkserver.rpcTimeoutMS",
        &fileServiceOption_.ioOpt.ioSenderOpt.failRequestOpt.chunkserverRPCTimeoutMS);         // NOLINT
    LOG_IF(ERROR, ret == false) << "config no chunkserver.rpcTimeoutMS info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt64Value("chunkserver.maxRetrySleepIntervalUS",
        &fileServiceOption_.ioOpt.ioSenderOpt.failRequestOpt.chunkserverMaxRetrySleepIntervalUS);   // NOLINT
    LOG_IF(ERROR, ret == false) << "config no chunkserver.maxRetrySleepIntervalUS info";   // NOLINT

    ret = conf_.GetUInt64Value("chunkserver.maxRPCTimeoutMS",
        &fileServiceOption_.ioOpt.ioSenderOpt.failRequestOpt.chunkserverMaxRPCTimeoutMS);   // NOLINT
    LOG_IF(ERROR, ret == false) << "config no chunkserver.maxRPCTimeoutMS info";

    ret = conf_.GetUInt64Value("chunkserver.maxStableTimeoutTimes",
        &fileServiceOption_.ioOpt.ioSenderOpt.failRequestOpt.chunkserverMaxStableTimeoutTimes);  // NOLINT
    LOG_IF(ERROR, ret == false) << "config no chunkserver.maxStableTimeoutTimes info";   //  NOLINT
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt64Value("chunkserver.minRetryTimesForceTimeoutBackoff",
        &fileServiceOption_.ioOpt.ioSenderOpt.failRequestOpt.chunkserverMinRetryTimesForceTimeoutBackoff);  // NOLINT
    LOG_IF(ERROR, ret == false)
        << "config no chunkserver.minRetryTimesForceTimeoutBackoff "
        << "using default value";

    ret = conf_.GetUInt64Value("chunkserver.maxRetryTimesBeforeConsiderSuspend",
        &fileServiceOption_.ioOpt.ioSenderOpt.failRequestOpt.chunkserverMaxRetryTimesBeforeConsiderSuspend);   // NOLINT
    LOG_IF(ERROR, ret == false) << "config no chunkserver.maxRetryTimesBeforeConsiderSuspend info";             // NOLINT

    ret = conf_.GetUInt64Value("global.fileMaxInFlightRPCNum",
        &fileServiceOption_.ioOpt.ioSenderOpt.inflightOpt.fileMaxInFlightRPCNum);   // NOLINT
    LOG_IF(ERROR, ret == false) << "config no global.fileMaxInFlightRPCNum info";   // NOLINT
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("metacache.getLeaderRetry",
        &fileServiceOption_.ioOpt.metaCacheOpt.metacacheGetLeaderRetry);
    LOG_IF(ERROR, ret == false) << "config no metacache.getLeaderRetry info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("metacache.rpcRetryIntervalUS",
        &fileServiceOption_.ioOpt.metaCacheOpt.metacacheRPCRetryIntervalUS);
    LOG_IF(ERROR, ret == false) << "config no metacache.rpcRetryIntervalUS info";   // NOLINT
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("metacache.getLeaderTimeOutMS",
        &fileServiceOption_.ioOpt.metaCacheOpt.metacacheGetLeaderRPCTimeOutMS);
    LOG_IF(ERROR, ret == false) << "config no metacache.getLeaderTimeOutMS info";   // NOLINT
    RETURN_IF_FALSE(ret)

    ret = conf_.GetStringValue("metacache.getLeaderBackupRequestLbName",
        &fileServiceOption_.ioOpt.metaCacheOpt.metacacheGetLeaderBackupRequestLbName);  // NOLINT
    LOG_IF(ERROR, ret == false)
        << "config no metacacheGetLeaderBackupRequestLbName info, using default value"; // NOLINT

    ret = conf_.GetUInt32Value("metacache.getLeaderBackupRequestMS",
        &fileServiceOption_.ioOpt.metaCacheOpt.metacacheGetLeaderBackupRequestMS);   // NOLINT
    LOG_IF(ERROR, ret == false) << "config no metacache.getLeaderBackupRequestMS info, using default value";  // NOLINT

    ret = conf_.GetUInt32Value("schedule.queueCapacity",
        &fileServiceOption_.ioOpt.reqSchdulerOpt.scheduleQueueCapacity);
    LOG_IF(ERROR, ret == false) << "config no schedule.queueCapacity info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("schedule.threadpoolSize",
        &fileServiceOption_.ioOpt.reqSchdulerOpt.scheduleThreadpoolSize);
    LOG_IF(ERROR, ret == false) << "config no schedule.threadpoolSize info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("mds.refreshTimesPerLease",
        &fileServiceOption_.leaseOpt.mdsRefreshTimesPerLease);
    LOG_IF(ERROR, ret == false) << "config no mds.refreshTimesPerLease info";
    RETURN_IF_FALSE(ret)

    fileServiceOption_.ioOpt.reqSchdulerOpt.ioSenderOpt
    = fileServiceOption_.ioOpt.ioSenderOpt;


    ret = conf_.GetUInt64Value("isolation.taskQueueCapacity",
        &fileServiceOption_.ioOpt.taskThreadOpt.isolationTaskQueueCapacity);
    LOG_IF(ERROR, ret == false) << "config no isolation.taskQueueCapacity info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("isolation.taskThreadPoolSize",
        &fileServiceOption_.ioOpt.taskThreadOpt.isolationTaskThreadPoolSize);
    LOG_IF(ERROR, ret == false) << "config no isolation.taskThreadPoolSize info";   // NOLINT
    RETURN_IF_FALSE(ret)

    std::string metaAddr;
    ret = conf_.GetStringValue("mds.listen.addr", &metaAddr);
    LOG_IF(ERROR, ret == false) << "config no mds.listen.addr info";
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

    ret = conf_.GetUInt64Value("mds.rpcTimeoutMS",
        &fileServiceOption_.metaServerOpt.mdsRPCTimeoutMs);
    LOG_IF(ERROR, ret == false) << "config no mds.rpcTimeoutMS info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt32Value("mds.rpcRetryIntervalUS",
        &fileServiceOption_.metaServerOpt.mdsRPCRetryIntervalUS);
    LOG_IF(ERROR, ret == false) << "config no mds.rpcRetryIntervalUS info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt64Value("mds.maxRPCTimeoutMS",
        &fileServiceOption_.metaServerOpt.mdsMaxRPCTimeoutMS);
    LOG_IF(ERROR, ret == false) << "config no mds.maxRPCTimeoutMS info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetUInt64Value("mds.maxRetryMS",
        &fileServiceOption_.metaServerOpt.mdsMaxRetryMS);
    LOG_IF(WARNING, ret == false) << "config no mds.maxRetryMS info";

    ret = conf_.GetUInt32Value("mds.maxFailedTimesBeforeChangeMDS",
        &fileServiceOption_.metaServerOpt.mdsMaxFailedTimesBeforeChangeMDS);
    LOG_IF(ERROR, ret == false) << "config no mds.maxFailedTimesBeforeChangeMDS info";  // NOLINT

    ret = conf_.GetBoolValue("mds.registerToMDS",
        &fileServiceOption_.commonOpt.mdsRegisterToMDS);
    LOG_IF(ERROR, ret == false) << "config no mds.registerToMDS info";
    RETURN_IF_FALSE(ret)

    ret = conf_.GetStringValue("global.sessionMapPath",
        &fileServiceOption_.sessionmapOpt.sessionmap_path);
    LOG_IF(ERROR, ret == false) << "config no global.sessionMapPath info";

    return 0;
}

FileServiceOption_t ClientConfig::GetFileServiceOption() {
    return fileServiceOption_;
}

uint16_t ClientConfig::GetDummyserverStartPort() {
    return conf_.GetIntValue("global.metricDummyServerStartPort", 9000);
}

}   // namespace client
}   // namespace curve

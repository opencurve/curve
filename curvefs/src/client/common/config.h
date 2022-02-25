/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_SRC_CLIENT_COMMON_CONFIG_H_
#define CURVEFS_SRC_CLIENT_COMMON_CONFIG_H_

#include <string>

#include "curvefs/src/client/common/common.h"
#include "src/client/config_info.h"
#include "src/common/configuration.h"
#include "src/common/s3_adapter.h"

using ::curve::common::Configuration;
using ::curve::common::S3AdapterOption;
using ::curvefs::client::common::DiskCacheType;

namespace curvefs {
namespace client {
namespace common {

using MdsOption = ::curve::client::MetaServerOption;

struct BlockDeviceClientOptions {
    // config path
    std::string configPath;
};

struct MetaCacheOpt {
    int metacacheGetLeaderRetry = 3;
    int metacacheRPCRetryIntervalUS = 500;
    int metacacheGetLeaderRPCTimeOutMS = 1000;

    uint16_t getPartitionCountOnce = 3;
    uint16_t createPartitionOnce = 3;
};

struct ExcutorOpt {
    uint32_t maxRetry = 3;
    uint64_t retryIntervalUS = 200;
    uint64_t rpcTimeoutMS = 1000;
    uint64_t maxRPCTimeoutMS = 64000;
    uint64_t maxRetrySleepIntervalUS = 64ull * 1000 * 1000;
    uint64_t minRetryTimesForceTimeoutBackoff = 5;
    uint64_t maxRetryTimesBeforeConsiderSuspend = 20;
};

struct SpaceAllocServerOption {
    std::string spaceaddr;
    uint64_t rpcTimeoutMs;
};

struct DiskCacheOption {
    DiskCacheType diskCacheType;
    // cache disk dir
    std::string cacheDir;
    // if true, call fdatasync after write
    bool forceFlush;
    // trim interval
    uint64_t trimCheckIntervalSec;
    // async load interval
    uint64_t asyncLoadPeriodMs;
    // trim start if disk usage over fullRatio
    uint64_t fullRatio;
    // trim finish until disk usage below safeRatio
    uint64_t safeRatio;
    // the max size disk cache can use
    uint64_t maxUsableSpaceBytes;
    // the max time system command can run
    uint32_t cmdTimeoutSec;
    // the write throttle bps of disk cache
    uint64_t avgFlushBytes;
    // the write burst bps of disk cache
    uint64_t burstFlushBytes;
    // the times that write burst bps can continue
    uint64_t burstSecs;
    // the read throttle bps of disk cache
    uint64_t avgReadFileBytes;
    // the write throttle iops of disk cache
    uint64_t avgFlushIops;
    // the read throttle iops of disk cache
    uint64_t avgReadFileIops;
};

struct S3ClientAdaptorOption {
    uint64_t blockSize;
    uint64_t chunkSize;
    uint32_t fuseMaxSize;
    uint64_t pageSize;
    uint32_t prefetchBlocks;
    uint32_t prefetchExecQueueNum;
    uint32_t intervalSec;
    uint32_t flushIntervalSec;
    uint64_t writeCacheMaxByte;
    uint64_t readCacheMaxByte;
    uint32_t nearfullRatio;
    uint32_t baseSleepUs;
    DiskCacheOption diskCacheOpt;
};

struct S3Option {
    S3ClientAdaptorOption s3ClientAdaptorOpt;
    S3AdapterOption s3AdaptrOpt;
};

struct VolumeOption {
    uint64_t bigFileSize;
    uint64_t volBlockSize;
    uint64_t fsBlockSize;
};

struct ExtentManagerOption {
    uint64_t preAllocSize;
};

struct FuseClientOption {
    MdsOption mdsOpt;
    MetaCacheOpt metaCacheOpt;
    ExcutorOpt excutorOpt;
    SpaceAllocServerOption spaceOpt;
    BlockDeviceClientOptions bdevOpt;
    S3Option s3Opt;
    ExtentManagerOption extentManagerOpt;
    VolumeOption volumeOpt;

    uint32_t flushRetryIntervalMS;
    double attrTimeOut;
    double entryTimeOut;
    uint32_t listDentryLimit;
    uint32_t flushPeriodSec;
    uint32_t maxNameLength;
    uint64_t iCacheLruSize;
    uint64_t dCacheLruSize;
    bool enableICacheMetrics;
    bool enableDCacheMetrics;

    uint32_t dummyServerStartPort;
};

void InitFuseClientOption(Configuration *conf, FuseClientOption *clientOption);
}  // namespace common
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_COMMON_CONFIG_H_

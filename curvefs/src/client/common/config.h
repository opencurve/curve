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
#include "src/common/configuration.h"
#include "src/common/s3_adapter.h"
#include "src/client/config_info.h"

using ::curve::common::Configuration;
using ::curve::common::S3AdapterOption; 

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

struct S3ClientAdaptorOption {
    uint64_t blockSize;
    uint64_t chunkSize;
    uint32_t intervalSec;
    uint32_t flushInterval;
    uint64_t writeCacheMaxByte;
    uint64_t lruCapacity;
    uint64_t trimCheckInterval;
    uint64_t fullRatio;
    uint64_t safeRatio;
    std::string cacheDir;
    bool forceFlush;
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

    double attrTimeOut;
    double entryTimeOut;
    uint32_t listDentryLimit;
};

void InitFuseClientOption(Configuration *conf, FuseClientOption *clientOption);
}  // namespace common
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_COMMON_CONFIG_H_

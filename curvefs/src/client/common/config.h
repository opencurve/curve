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

#include <cstdint>
#include <string>

#include "curvefs/src/client/common/common.h"
#include "curvefs/proto/common.pb.h"
#include "src/client/config_info.h"
#include "src/common/configuration.h"
#include "src/common/s3_adapter.h"

using ::curve::common::Configuration;
using ::curve::common::S3AdapterOption;
using ::curvefs::client::common::DiskCacheType;
using ::curve::common::S3InfoOption;

namespace curvefs {
namespace client {
namespace common {

using MdsOption = ::curve::client::MetaServerOption;

struct BlockDeviceClientOptions {
    std::string configPath;
    uint32_t threadnum;
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
    uint64_t rpcStreamIdleTimeoutMS = 500;
    uint64_t maxRPCTimeoutMS = 64000;
    uint64_t maxRetrySleepIntervalUS = 64ull * 1000 * 1000;
    uint64_t minRetryTimesForceTimeoutBackoff = 5;
    uint64_t maxRetryTimesBeforeConsiderSuspend = 20;
    uint32_t batchInodeAttrLimit = 10000;
    bool enableRenameParallel = false;
};

struct LeaseOpt {
    uint32_t refreshTimesPerLease = 5;
    // default = 20s
    uint32_t leaseTimeUs = 20000000;
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
    // threads for disk cache
    uint32_t threads;
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
    uint32_t chunkFlushThreads;
    uint32_t flushIntervalSec;
    uint64_t writeCacheMaxByte;
    uint64_t readCacheMaxByte;
    uint32_t nearfullRatio;
    uint32_t baseSleepUs;
    uint32_t maxReadRetryIntervalMs;
    uint32_t readRetryIntervalMs;
    DiskCacheOption diskCacheOpt;
};

struct S3Option {
    S3ClientAdaptorOption s3ClientAdaptorOpt;
    S3AdapterOption s3AdaptrOpt;
};

struct BlockGroupOption {
    uint32_t allocateOnce;
};

struct BitmapAllocatorOption {
    uint64_t sizePerBit;
    double smallAllocProportion;
};

struct VolumeAllocatorOption {
    std::string type;
    BitmapAllocatorOption bitmapAllocatorOption;
    BlockGroupOption blockGroupOption;
};

struct VolumeOption {
    uint64_t bigFileSize;
    uint64_t volBlockSize;
    uint64_t fsBlockSize;
    VolumeAllocatorOption allocatorOption;
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
    LeaseOpt leaseOpt;

    double attrTimeOut;
    double entryTimeOut;
    uint32_t listDentryLimit;
    uint32_t listDentryThreads;
    uint32_t flushPeriodSec;
    uint32_t maxNameLength;
    uint64_t iCacheLruSize;
    uint64_t dCacheLruSize;
    bool enableICacheMetrics;
    bool enableDCacheMetrics;
    uint32_t dummyServerStartPort;
    bool enableMultiMountPointRename = false;
    bool enableFuseSplice = false;
    bool disableXattr = false;
};

void InitFuseClientOption(Configuration *conf, FuseClientOption *clientOption);

void SetFuseClientS3Option(FuseClientOption *clientOption,
    const S3InfoOption &fsS3Opt);

void S3Info2FsS3Option(const curvefs::common::S3Info& s3,
                       S3InfoOption* fsS3Opt);

void InitMdsOption(Configuration *conf, MdsOption *mdsOpt);

void InitLeaseOpt(Configuration *conf, LeaseOpt *leaseOpt);

}  // namespace common
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_COMMON_CONFIG_H_

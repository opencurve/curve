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

#include "curvefs/src/client/common/config.h"

#include <gflags/gflags.h>

#include <string>
#include <vector>

#include "src/common/gflags_helper.h"
#include "src/common/string_util.h"

namespace brpc {
DECLARE_int32(defer_close_second);
DECLARE_int32(health_check_interval);
}  // namespace brpc

namespace curvefs {
namespace client {
namespace common {
DEFINE_bool(enableCto, true, "acheieve cto consistency");

void InitMdsOption(Configuration *conf, MdsOption *mdsOpt) {
    conf->GetValueFatalIfFail("mdsOpt.mdsMaxRetryMS", &mdsOpt->mdsMaxRetryMS);
    conf->GetValueFatalIfFail("mdsOpt.rpcRetryOpt.maxRPCTimeoutMS",
                              &mdsOpt->rpcRetryOpt.maxRPCTimeoutMS);
    conf->GetValueFatalIfFail("mdsOpt.rpcRetryOpt.rpcTimeoutMs",
                              &mdsOpt->rpcRetryOpt.rpcTimeoutMs);
    conf->GetValueFatalIfFail("mdsOpt.rpcRetryOpt.rpcRetryIntervalUS",
                              &mdsOpt->rpcRetryOpt.rpcRetryIntervalUS);
    conf->GetValueFatalIfFail(
        "mdsOpt.rpcRetryOpt.maxFailedTimesBeforeChangeAddr",
        &mdsOpt->rpcRetryOpt.maxFailedTimesBeforeChangeAddr);
    conf->GetValueFatalIfFail(
        "mdsOpt.rpcRetryOpt.normalRetryTimesBeforeTriggerWait",
        &mdsOpt->rpcRetryOpt.normalRetryTimesBeforeTriggerWait);
    conf->GetValueFatalIfFail("mdsOpt.rpcRetryOpt.waitSleepMs",
                              &mdsOpt->rpcRetryOpt.waitSleepMs);
    std::string adds;
    conf->GetValueFatalIfFail("mdsOpt.rpcRetryOpt.addrs", &adds);

    std::vector<std::string> mdsAddr;
    curve::common::SplitString(adds, ",", &mdsAddr);
    mdsOpt->rpcRetryOpt.addrs.assign(mdsAddr.begin(), mdsAddr.end());
}

void InitMetaCacheOption(Configuration *conf, MetaCacheOpt *opts) {
    conf->GetValueFatalIfFail("metaCacheOpt.metacacheGetLeaderRetry",
                              &opts->metacacheGetLeaderRetry);
    conf->GetValueFatalIfFail("metaCacheOpt.metacacheRPCRetryIntervalUS",
                              &opts->metacacheRPCRetryIntervalUS);
    conf->GetValueFatalIfFail("metaCacheOpt.metacacheGetLeaderRPCTimeOutMS",
                              &opts->metacacheGetLeaderRPCTimeOutMS);
}

void InitExcutorOption(Configuration *conf, ExcutorOpt *opts) {
    conf->GetValueFatalIfFail("excutorOpt.maxRetry", &opts->maxRetry);
    conf->GetValueFatalIfFail("excutorOpt.retryIntervalUS",
                              &opts->retryIntervalUS);
    conf->GetValueFatalIfFail("excutorOpt.rpcTimeoutMS", &opts->rpcTimeoutMS);
    conf->GetValueFatalIfFail("excutorOpt.maxRPCTimeoutMS",
                              &opts->maxRPCTimeoutMS);
    conf->GetValueFatalIfFail("excutorOpt.maxRetrySleepIntervalUS",
                              &opts->maxRetrySleepIntervalUS);
    conf->GetValueFatalIfFail("excutorOpt.minRetryTimesForceTimeoutBackoff",
                              &opts->minRetryTimesForceTimeoutBackoff);
    conf->GetValueFatalIfFail("excutorOpt.maxRetryTimesBeforeConsiderSuspend",
                              &opts->maxRetryTimesBeforeConsiderSuspend);
    conf->GetValueFatalIfFail("excutorOpt.batchLimit", &opts->batchLimit);
}

void InitSpaceServerOption(Configuration *conf,
                           SpaceAllocServerOption *spaceOpt) {
    conf->GetValueFatalIfFail("spaceserver.spaceaddr", &spaceOpt->spaceaddr);
    conf->GetValueFatalIfFail("spaceserver.rpcTimeoutMs",
                              &spaceOpt->rpcTimeoutMs);
}

void InitBlockDeviceOption(Configuration *conf,
                           BlockDeviceClientOptions *bdevOpt) {
    conf->GetValueFatalIfFail("bdev.confpath", &bdevOpt->configPath);
}

void InitDiskCacheOption(Configuration *conf,
                         DiskCacheOption *diskCacheOption) {
    uint32_t diskCacheType;
    conf->GetValueFatalIfFail("diskCache.diskCacheType",
                              &diskCacheType);
    diskCacheOption->diskCacheType = (DiskCacheType)diskCacheType;
    conf->GetValueFatalIfFail("diskCache.forceFlush",
                              &diskCacheOption->forceFlush);
    conf->GetValueFatalIfFail("diskCache.cacheDir", &diskCacheOption->cacheDir);
    conf->GetValueFatalIfFail("diskCache.trimCheckIntervalSec",
                              &diskCacheOption->trimCheckIntervalSec);
    conf->GetValueFatalIfFail("diskCache.asyncLoadPeriodMs",
                              &diskCacheOption->asyncLoadPeriodMs);
    conf->GetValueFatalIfFail("diskCache.fullRatio",
                              &diskCacheOption->fullRatio);
    conf->GetValueFatalIfFail("diskCache.safeRatio",
                              &diskCacheOption->safeRatio);
    conf->GetValueFatalIfFail("diskCache.maxUsableSpaceBytes",
                              &diskCacheOption->maxUsableSpaceBytes);
    conf->GetValueFatalIfFail("diskCache.cmdTimeoutSec",
                              &diskCacheOption->cmdTimeoutSec);
    conf->GetValueFatalIfFail("diskCache.avgFlushBytes",
                              &diskCacheOption->avgFlushBytes);
    conf->GetValueFatalIfFail("diskCache.burstFlushBytes",
                              &diskCacheOption->burstFlushBytes);
    conf->GetValueFatalIfFail("diskCache.burstSecs",
                              &diskCacheOption->burstSecs);
    conf->GetValueFatalIfFail("diskCache.avgFlushIops",
                              &diskCacheOption->avgFlushIops);
    conf->GetValueFatalIfFail("diskCache.avgReadFileBytes",
                              &diskCacheOption->avgReadFileBytes);
    conf->GetValueFatalIfFail("diskCache.avgReadFileIops",
                              &diskCacheOption->avgReadFileIops);
}

void InitS3Option(Configuration *conf, S3Option *s3Opt) {
    conf->GetValueFatalIfFail("s3.fuseMaxSize",
                              &s3Opt->s3ClientAdaptorOpt.fuseMaxSize);
    conf->GetValueFatalIfFail("s3.pagesize",
                              &s3Opt->s3ClientAdaptorOpt.pageSize);
    conf->GetValueFatalIfFail("s3.prefetchBlocks",
                              &s3Opt->s3ClientAdaptorOpt.prefetchBlocks);
    conf->GetValueFatalIfFail("s3.prefetchExecQueueNum",
                              &s3Opt->s3ClientAdaptorOpt.prefetchExecQueueNum);
    conf->GetValueFatalIfFail("s3.intervalSec",
                              &s3Opt->s3ClientAdaptorOpt.intervalSec);
    conf->GetValueFatalIfFail("s3.flushIntervalSec",
                              &s3Opt->s3ClientAdaptorOpt.flushIntervalSec);
    conf->GetValueFatalIfFail("s3.writeCacheMaxByte",
                              &s3Opt->s3ClientAdaptorOpt.writeCacheMaxByte);
    conf->GetValueFatalIfFail("s3.readCacheMaxByte",
                              &s3Opt->s3ClientAdaptorOpt.readCacheMaxByte);
    conf->GetValueFatalIfFail("s3.nearfullRatio",
                              &s3Opt->s3ClientAdaptorOpt.nearfullRatio);
    conf->GetValueFatalIfFail("s3.baseSleepUs",
                              &s3Opt->s3ClientAdaptorOpt.baseSleepUs);
    ::curve::common::InitS3AdaptorOptionExceptS3InfoOption(conf,
                                                         &s3Opt->s3AdaptrOpt);
    InitDiskCacheOption(conf, &s3Opt->s3ClientAdaptorOpt.diskCacheOpt);
}

void InitVolumeOption(Configuration *conf, VolumeOption *volumeOpt) {
    conf->GetValueFatalIfFail("volume.bigFileSize", &volumeOpt->bigFileSize);
    conf->GetValueFatalIfFail("volume.volBlockSize", &volumeOpt->volBlockSize);
    conf->GetValueFatalIfFail("volume.fsBlockSize", &volumeOpt->fsBlockSize);
}

void InitExtentManagerOption(Configuration *conf,
                             ExtentManagerOption *extentManagerOpt) {
    conf->GetValueFatalIfFail("extentManager.preAllocSize",
                              &extentManagerOpt->preAllocSize);
}

void InitLeaseOpt(Configuration *conf, LeaseOpt *leaseOpt) {
    conf->GetValueFatalIfFail("mds.leaseTimesUs", &leaseOpt->leaseTimeUs);
    conf->GetValueFatalIfFail("mds.refreshTimesPerLease",
                              &leaseOpt->refreshTimesPerLease);
}

void SetBrpcOpt(Configuration *conf) {
    curve::common::GflagsLoadValueFromConfIfCmdNotSet dummy;
    dummy.Load(conf, "defer_close_second", "rpc.defer.close.second",
               &brpc::FLAGS_defer_close_second);
    dummy.Load(conf, "health_check_interval", "rpc.healthCheckIntervalSec",
               &brpc::FLAGS_health_check_interval);
}

void InitFuseClientOption(Configuration *conf, FuseClientOption *clientOption) {
    InitMdsOption(conf, &clientOption->mdsOpt);
    InitMetaCacheOption(conf, &clientOption->metaCacheOpt);
    InitExcutorOption(conf, &clientOption->excutorOpt);
    InitSpaceServerOption(conf, &clientOption->spaceOpt);
    InitBlockDeviceOption(conf, &clientOption->bdevOpt);
    InitS3Option(conf, &clientOption->s3Opt);
    InitExtentManagerOption(conf, &clientOption->extentManagerOpt);
    InitVolumeOption(conf, &clientOption->volumeOpt);
    InitLeaseOpt(conf, &clientOption->leaseOpt);

    conf->GetValueFatalIfFail("fuseClient.attrTimeOut",
                              &clientOption->attrTimeOut);
    conf->GetValueFatalIfFail("fuseClient.entryTimeOut",
                              &clientOption->entryTimeOut);
    conf->GetValueFatalIfFail("fuseClient.listDentryLimit",
                              &clientOption->listDentryLimit);
    conf->GetValueFatalIfFail("fuseClient.flushPeriodSec",
                              &clientOption->flushPeriodSec);
    conf->GetValueFatalIfFail("fuseClient.maxNameLength",
                              &clientOption->maxNameLength);
    conf->GetValueFatalIfFail("fuseClient.iCacheLruSize",
                              &clientOption->iCacheLruSize);
    conf->GetValueFatalIfFail("fuseClient.dCacheLruSize",
                              &clientOption->dCacheLruSize);
    conf->GetValueFatalIfFail("fuseClient.enableICacheMetrics",
                              &clientOption->enableICacheMetrics);
    conf->GetValueFatalIfFail("fuseClient.enableDCacheMetrics",
                              &clientOption->enableDCacheMetrics);
    conf->GetValueFatalIfFail("fuseClient.cto", &FLAGS_enableCto);
    conf->GetValueFatalIfFail("client.dummyserver.startport",
                              &clientOption->dummyServerStartPort);

    SetBrpcOpt(conf);
}

void SetFuseClientS3Option(FuseClientOption *clientOption,
    const S3InfoOption &fsS3Opt) {
    clientOption->s3Opt.s3ClientAdaptorOpt.blockSize = fsS3Opt.blockSize;
    clientOption->s3Opt.s3ClientAdaptorOpt.chunkSize = fsS3Opt.chunkSize;
    clientOption->s3Opt.s3AdaptrOpt.s3Address = fsS3Opt.s3Address;
    clientOption->s3Opt.s3AdaptrOpt.ak = fsS3Opt.ak;
    clientOption->s3Opt.s3AdaptrOpt.sk = fsS3Opt.sk;
    clientOption->s3Opt.s3AdaptrOpt.bucketName = fsS3Opt.bucketName;
}

void S3Info2FsS3Option(const curvefs::common::S3Info& s3,
                       S3InfoOption* fsS3Opt) {
    fsS3Opt->ak = s3.ak();
    fsS3Opt->sk = s3.sk();
    fsS3Opt->s3Address = s3.endpoint();
    fsS3Opt->bucketName = s3.bucketname();
    fsS3Opt->blockSize = s3.blocksize();
    fsS3Opt->chunkSize = s3.chunksize();
}

}  // namespace common
}  // namespace client
}  // namespace curvefs

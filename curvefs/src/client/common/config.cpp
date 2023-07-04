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
DECLARE_bool(useFakeS3);
}  // namespace common
}  // namespace client
}  // namespace curvefs

namespace curvefs {
namespace client {
namespace common {
DEFINE_bool(enableCto, true, "acheieve cto consistency");
DEFINE_bool(useFakeS3, false,
            "Use fake s3 to inject more metadata for testing metaserver");
DEFINE_bool(supportKVcache, false, "use kvcache to speed up sharing");
DEFINE_bool(access_logging, true, "enable access log");

/**
 * use curl -L fuseclient:port/flags/fuseClientAvgWriteBytes?setvalue=true
 * for dynamic parameter configuration
 */
static bool pass_uint64(const char *, uint64_t) { return true; }

DEFINE_uint64(fuseClientAvgWriteBytes, 0,
              "the write throttle bps of fuse client");
DEFINE_validator(fuseClientAvgWriteBytes, &pass_uint64);
DEFINE_uint64(fuseClientBurstWriteBytes, 0,
              "the write burst bps of fuse client");
DEFINE_validator(fuseClientBurstWriteBytes, &pass_uint64);
DEFINE_uint64(fuseClientBurstWriteBytesSecs, 180,
              "the times that write burst bps can continue");
DEFINE_validator(fuseClientBurstWriteBytesSecs, &pass_uint64);


DEFINE_uint64(fuseClientAvgWriteIops, 0,
              "the write throttle iops of fuse client");
DEFINE_validator(fuseClientAvgWriteIops, &pass_uint64);
DEFINE_uint64(fuseClientBurstWriteIops, 0,
              "the write burst iops of fuse client");
DEFINE_validator(fuseClientBurstWriteIops, &pass_uint64);
DEFINE_uint64(fuseClientBurstWriteIopsSecs, 180,
              "the times that write burst iops can continue");
DEFINE_validator(fuseClientBurstWriteIopsSecs, &pass_uint64);


DEFINE_uint64(fuseClientAvgReadBytes, 0,
              "the Read throttle bps of fuse client");
DEFINE_validator(fuseClientAvgReadBytes, &pass_uint64);
DEFINE_uint64(fuseClientBurstReadBytes, 0,
              "the Read burst bps of fuse client");
DEFINE_validator(fuseClientBurstReadBytes, &pass_uint64);
DEFINE_uint64(fuseClientBurstReadBytesSecs, 180,
              "the times that Read burst bps can continue");
DEFINE_validator(fuseClientBurstReadBytesSecs, &pass_uint64);


DEFINE_uint64(fuseClientAvgReadIops, 0,
              "the Read throttle iops of fuse client");
DEFINE_validator(fuseClientAvgReadIops, &pass_uint64);
DEFINE_uint64(fuseClientBurstReadIops, 0,
              "the Read burst iops of fuse client");
DEFINE_validator(fuseClientBurstReadIops, &pass_uint64);
DEFINE_uint64(fuseClientBurstReadIopsSecs, 180,
              "the times that Read burst iops can continue");
DEFINE_validator(fuseClientBurstReadIopsSecs, &pass_uint64);

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

void InitExcutorOption(Configuration *conf, ExcutorOpt *opts, bool internal) {
    if (internal) {
        conf->GetValueFatalIfFail("executorOpt.maxInternalRetry",
                                  &opts->maxRetry);
    } else {
        conf->GetValueFatalIfFail("executorOpt.maxRetry", &opts->maxRetry);
    }

    conf->GetValueFatalIfFail("executorOpt.retryIntervalUS",
                              &opts->retryIntervalUS);
    conf->GetValueFatalIfFail("executorOpt.rpcTimeoutMS", &opts->rpcTimeoutMS);
    conf->GetValueFatalIfFail("executorOpt.rpcStreamIdleTimeoutMS",
                              &opts->rpcStreamIdleTimeoutMS);
    conf->GetValueFatalIfFail("executorOpt.maxRPCTimeoutMS",
                              &opts->maxRPCTimeoutMS);
    conf->GetValueFatalIfFail("executorOpt.maxRetrySleepIntervalUS",
                              &opts->maxRetrySleepIntervalUS);
    conf->GetValueFatalIfFail("executorOpt.minRetryTimesForceTimeoutBackoff",
                              &opts->minRetryTimesForceTimeoutBackoff);
    conf->GetValueFatalIfFail("executorOpt.maxRetryTimesBeforeConsiderSuspend",
                              &opts->maxRetryTimesBeforeConsiderSuspend);
    conf->GetValueFatalIfFail("executorOpt.batchInodeAttrLimit",
                              &opts->batchInodeAttrLimit);
    conf->GetValueFatalIfFail("fuseClient.enableMultiMountPointRename",
                              &opts->enableRenameParallel);
}

void InitBlockDeviceOption(Configuration *conf,
                           BlockDeviceClientOptions *bdevOpt) {
    conf->GetValueFatalIfFail("bdev.confPath", &bdevOpt->configPath);
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
    conf->GetValueFatalIfFail("diskCache.trimRatio",
                              &diskCacheOption->trimRatio);
    conf->GetValueFatalIfFail("diskCache.maxUsableSpaceBytes",
                              &diskCacheOption->maxUsableSpaceBytes);
    conf->GetValueFatalIfFail("diskCache.maxFileNums",
                              &diskCacheOption->maxFileNums);
    conf->GetValueFatalIfFail("diskCache.cmdTimeoutSec",
                              &diskCacheOption->cmdTimeoutSec);
    conf->GetValueFatalIfFail("diskCache.threads",
                              &diskCacheOption->threads);
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
    conf->GetValueFatalIfFail("s3.fakeS3", &FLAGS_useFakeS3);
    conf->GetValueFatalIfFail("s3.pageSize",
                              &s3Opt->s3ClientAdaptorOpt.pageSize);
    conf->GetValueFatalIfFail("s3.prefetchBlocks",
                              &s3Opt->s3ClientAdaptorOpt.prefetchBlocks);
    conf->GetValueFatalIfFail("s3.prefetchExecQueueNum",
                              &s3Opt->s3ClientAdaptorOpt.prefetchExecQueueNum);
    conf->GetValueFatalIfFail("s3.threadScheduleInterval",
                              &s3Opt->s3ClientAdaptorOpt.intervalSec);
    conf->GetValueFatalIfFail("s3.cacheFlushIntervalSec",
                              &s3Opt->s3ClientAdaptorOpt.flushIntervalSec);
    conf->GetValueFatalIfFail("s3.chunkFlushThreads",
                              &s3Opt->s3ClientAdaptorOpt.chunkFlushThreads);
    conf->GetValueFatalIfFail("s3.writeCacheMaxByte",
                              &s3Opt->s3ClientAdaptorOpt.writeCacheMaxByte);
    conf->GetValueFatalIfFail("s3.readCacheMaxByte",
                              &s3Opt->s3ClientAdaptorOpt.readCacheMaxByte);
    conf->GetValueFatalIfFail("s3.readCacheThreads",
                              &s3Opt->s3ClientAdaptorOpt.readCacheThreads);
    conf->GetValueFatalIfFail("s3.nearfullRatio",
                              &s3Opt->s3ClientAdaptorOpt.nearfullRatio);
    conf->GetValueFatalIfFail("s3.baseSleepUs",
                              &s3Opt->s3ClientAdaptorOpt.baseSleepUs);
    conf->GetValueFatalIfFail(
        "s3.maxReadRetryIntervalMs",
        &s3Opt->s3ClientAdaptorOpt.maxReadRetryIntervalMs);
    conf->GetValueFatalIfFail("s3.readRetryIntervalMs",
                              &s3Opt->s3ClientAdaptorOpt.readRetryIntervalMs);
    ::curve::common::InitS3AdaptorOptionExceptS3InfoOption(conf,
                                                           &s3Opt->s3AdaptrOpt);
    InitDiskCacheOption(conf, &s3Opt->s3ClientAdaptorOpt.diskCacheOpt);
}

void InitVolumeOption(Configuration *conf, VolumeOption *volumeOpt) {
    conf->GetValueFatalIfFail("volume.bigFileSize", &volumeOpt->bigFileSize);
    conf->GetValueFatalIfFail("volume.volBlockSize", &volumeOpt->volBlockSize);
    conf->GetValueFatalIfFail("volume.fsBlockSize", &volumeOpt->fsBlockSize);
    conf->GetValueFatalIfFail("volume.allocator.type",
                              &volumeOpt->allocatorOption.type);
    conf->GetValueFatalIfFail("volume.space.useThreshold",
                              &volumeOpt->threshold);
    conf->GetValueFatalIfFail("volume.space.releaseInterSec",
                              &volumeOpt->releaseInterSec);

    conf->GetValueFatalIfFail(
        "volume.blockGroup.allocateOnce",
        &volumeOpt->allocatorOption.blockGroupOption.allocateOnce);

    if (volumeOpt->allocatorOption.type == "bitmap") {
        conf->GetValueFatalIfFail(
            "volume.bitmapAllocator.sizePerBit",
            &volumeOpt->allocatorOption.bitmapAllocatorOption.sizePerBit);
        conf->GetValueFatalIfFail(
            "volume.bitmapAllocator.smallAllocProportion",
            &volumeOpt->allocatorOption.bitmapAllocatorOption
                 .smallAllocProportion);
    } else {
        CHECK(false) << "only support bitmap allocator";
    }
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

void InitRefreshDataOpt(Configuration *conf,
                        RefreshDataOption *opt) {
    conf->GetValueFatalIfFail("fuseClient.maxDataSize",
                              &opt->maxDataSize);
    conf->GetValueFatalIfFail("fuseClient.refreshDataIntervalSec",
                              &opt->refreshDataIntervalSec);
}

void InitKVClientManagerOpt(Configuration *conf,
                               KVClientManagerOpt *config) {
    conf->GetValueFatalIfFail("fuseClient.supportKVcache",
                              &FLAGS_supportKVcache);
    conf->GetValueFatalIfFail("fuseClient.setThreadPool",
                              &config->setThreadPooln);
    conf->GetValueFatalIfFail("fuseClient.getThreadPool",
                              &config->getThreadPooln);
}

void GetGids(Configuration* c,
             const std::string& key,
             std::vector<uint32_t>* gids) {
    std::string str;
    std::vector<std::string> ss;
    c->GetValueFatalIfFail("vfs.userPermission.gids", &str);
    curve::common::SplitString(str, ",", &ss);
    uint32_t gid;
    for (const auto& s : ss) {
        LOG_IF(FATAL, !curve::common::StringToUl(s, &gid))
            << "Invalid `" << key << "`: <" << s << ">";
        gids->push_back(static_cast<uint32_t>(gid));
    }
}

void InitVFSOption(Configuration* c, VFSOption* option) {
    {  // vfs cache option
        auto o = &option->vfsCacheOption;
        c->GetValueFatalIfFail("vfs.entryCache.lruSize", &o->entryCacheLruSize);
        c->GetValueFatalIfFail("vfs.attrCache.lruSize", &o->attrCacheLruSize);
    }
    {  // user permission option
        auto o = &option->userPermissionOption;
        c->GetValueFatalIfFail("vfs.userPermission.uid", &o->uid);
        c->GetValueFatalIfFail("vfs.userPermission.umask", &o->umask);
        GetGids(c, "vfs.userPermission.gids", &o->gids);
    }
}

void InitFileSystemOption(Configuration* c, FileSystemOption* option) {
    c->GetValueFatalIfFail("fs.cto", &option->cto);
    c->GetValueFatalIfFail("fs.cto", &FLAGS_enableCto);
    c->GetValueFatalIfFail("fs.disableXattr", &option->disableXattr);
    c->GetValueFatalIfFail("fs.maxNameLength", &option->maxNameLength);
    c->GetValueFatalIfFail("fs.accessLogging", &FLAGS_access_logging);
    {  // kernel cache option
        auto o = &option->kernelCacheOption;
        c->GetValueFatalIfFail("fs.kernelCache.attrTimeoutSec",
                               &o->attrTimeoutSec);
        c->GetValueFatalIfFail("fs.kernelCache.dirAttrTimeoutSec",
                               &o->dirAttrTimeoutSec);
        c->GetValueFatalIfFail("fs.kernelCache.entryTimeoutSec",
                               &o->entryTimeoutSec);
        c->GetValueFatalIfFail("fs.kernelCache.dirEntryTimeoutSec",
                               &o->dirEntryTimeoutSec);
    }
    {  // lookup cache option
        auto o = &option->lookupCacheOption;
        c->GetValueFatalIfFail("fs.lookupCache.lruSize",
                               &o->lruSize);
        c->GetValueFatalIfFail("fs.lookupCache.negativeTimeoutSec",
                               &o->negativeTimeoutSec);
        c->GetValueFatalIfFail("fs.lookupCache.minUses",
                               &o->minUses);
    }
    {  // dir cache option
        auto o = &option->dirCacheOption;
        c->GetValueFatalIfFail("fs.dirCache.lruSize", &o->lruSize);
    }
    {  // open file option
        auto o = &option->openFilesOption;
        c->GetValueFatalIfFail("fs.openFile.lruSize", &o->lruSize);
    }
    {  // attr watcher option
        auto o = &option->attrWatcherOption;
        c->GetValueFatalIfFail("fs.attrWatcher.lruSize", &o->lruSize);
    }
    {  // rpc option
        auto o = &option->rpcOption;
        c->GetValueFatalIfFail("fs.rpc.listDentryLimit", &o->listDentryLimit);
    }
    {  // defer sync option
        auto o = &option->deferSyncOption;
        c->GetValueFatalIfFail("fs.deferSync.delay", &o->delay);
        c->GetValueFatalIfFail("fs.deferSync.deferDirMtime", &o->deferDirMtime);
    }
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
    InitExcutorOption(conf, &clientOption->excutorOpt, false);
    InitExcutorOption(conf, &clientOption->excutorInternalOpt, true);
    InitBlockDeviceOption(conf, &clientOption->bdevOpt);
    InitS3Option(conf, &clientOption->s3Opt);
    InitExtentManagerOption(conf, &clientOption->extentManagerOpt);
    InitVolumeOption(conf, &clientOption->volumeOpt);
    InitLeaseOpt(conf, &clientOption->leaseOpt);
    InitRefreshDataOpt(conf, &clientOption->refreshDataOption);
    InitKVClientManagerOpt(conf, &clientOption->kvClientManagerOpt);
    InitVFSOption(conf, &clientOption->vfsOption);
    InitFileSystemOption(conf, &clientOption->fileSystemOption);

    conf->GetValueFatalIfFail("fuseClient.listDentryLimit",
                              &clientOption->listDentryLimit);
    conf->GetValueFatalIfFail("fuseClient.listDentryThreads",
                              &clientOption->listDentryThreads);
    conf->GetValueFatalIfFail("client.dummyServer.startPort",
                              &clientOption->dummyServerStartPort);
    conf->GetValueFatalIfFail("fuseClient.enableMultiMountPointRename",
                              &clientOption->enableMultiMountPointRename);
    conf->GetValueFatalIfFail("fuseClient.downloadMaxRetryTimes",
                              &clientOption->downloadMaxRetryTimes);
    conf->GetValueFatalIfFail("fuseClient.warmupThreadsNum",
                              &clientOption->warmupThreadsNum);
    LOG_IF(WARNING, conf->GetBoolValue("fuseClient.enableSplice",
                                       &clientOption->enableFuseSplice))
        << "Not found `fuseClient.enableSplice` in conf, use default value `"
        << std::boolalpha << clientOption->enableFuseSplice << '`';

    conf->GetValueFatalIfFail("fuseClient.throttle.avgWriteBytes",
                              &FLAGS_fuseClientAvgWriteBytes);
    conf->GetValueFatalIfFail("fuseClient.throttle.burstWriteBytes",
                              &FLAGS_fuseClientBurstWriteBytes);
    conf->GetValueFatalIfFail("fuseClient.throttle.burstWriteBytesSecs",
                              &FLAGS_fuseClientBurstWriteBytesSecs);

    conf->GetValueFatalIfFail("fuseClient.throttle.avgWriteIops",
                              &FLAGS_fuseClientAvgWriteIops);
    conf->GetValueFatalIfFail("fuseClient.throttle.burstWriteIops",
                              &FLAGS_fuseClientBurstWriteIops);
    conf->GetValueFatalIfFail("fuseClient.throttle.burstWriteIopsSecs",
                              &FLAGS_fuseClientBurstWriteIopsSecs);

    conf->GetValueFatalIfFail("fuseClient.throttle.avgReadBytes",
                              &FLAGS_fuseClientAvgReadBytes);
    conf->GetValueFatalIfFail("fuseClient.throttle.burstReadBytes",
                              &FLAGS_fuseClientBurstReadBytes);
    conf->GetValueFatalIfFail("fuseClient.throttle.burstReadBytesSecs",
                              &FLAGS_fuseClientBurstReadBytesSecs);

    conf->GetValueFatalIfFail("fuseClient.throttle.avgReadIops",
                              &FLAGS_fuseClientAvgReadIops);
    conf->GetValueFatalIfFail("fuseClient.throttle.burstReadIops",
                              &FLAGS_fuseClientBurstReadIops);
    conf->GetValueFatalIfFail("fuseClient.throttle.burstReadIopsSecs",
                              &FLAGS_fuseClientBurstReadIopsSecs);
    SetBrpcOpt(conf);
}

void SetFuseClientS3Option(FuseClientOption *clientOption,
    const S3InfoOption &fsS3Opt) {
    clientOption->s3Opt.s3ClientAdaptorOpt.blockSize = fsS3Opt.blockSize;
    clientOption->s3Opt.s3ClientAdaptorOpt.chunkSize = fsS3Opt.chunkSize;
    clientOption->s3Opt.s3ClientAdaptorOpt.objectPrefix = fsS3Opt.objectPrefix;
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
    fsS3Opt->objectPrefix = s3.has_objectprefix() ? s3.objectprefix() : 0;
}

}  // namespace common
}  // namespace client
}  // namespace curvefs

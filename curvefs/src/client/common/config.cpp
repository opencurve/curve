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

#include <gflags/gflags.h>

#include <string>

#include "curvefs/src/client/common/config.h"

#include "src/common/string_util.h"

namespace brpc {
DECLARE_int32(defer_close_second);
}  // namespace brpc


namespace curvefs {
namespace client {
namespace common {
void InitMdsOption(Configuration *conf,
    MdsOption *mdsOpt) {
    conf->GetValueFatalIfFail("mdsOpt.mdsMaxRetryMS",
                              &mdsOpt->mdsMaxRetryMS);
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
    conf->GetValueFatalIfFail("mdsOpt.rpcRetryOpt.addrs",
                              &adds);

    std::vector<std::string> mdsAddr;
    curve::common::SplitString(adds, ",", &mdsAddr);
    mdsOpt->rpcRetryOpt.addrs.assign(mdsAddr.begin(), mdsAddr.end());
}

void InitMetaCacheOption(Configuration *conf,
    MetaCacheOpt *opts) {
    conf->GetValueFatalIfFail("metaCacheOpt.metacacheGetLeaderRetry",
                              &opts->metacacheGetLeaderRetry);
    conf->GetValueFatalIfFail("metaCacheOpt.metacacheRPCRetryIntervalUS",
                              &opts->metacacheRPCRetryIntervalUS);
    conf->GetValueFatalIfFail("metaCacheOpt.metacacheGetLeaderRPCTimeOutMS",
                              &opts->metacacheGetLeaderRPCTimeOutMS);
}

void InitExcutorOption(Configuration *conf,
    ExcutorOpt *opts) {
    conf->GetValueFatalIfFail("excutorOpt.maxRetry",
                              &opts->maxRetry);
    conf->GetValueFatalIfFail("excutorOpt.retryIntervalUS",
                              &opts->retryIntervalUS);
    conf->GetValueFatalIfFail("excutorOpt.rpcTimeoutMS",
                              &opts->rpcTimeoutMS);
    conf->GetValueFatalIfFail("excutorOpt.maxRPCTimeoutMS",
                              &opts->maxRPCTimeoutMS);
    conf->GetValueFatalIfFail("excutorOpt.maxRetrySleepIntervalUS",
                              &opts->maxRetrySleepIntervalUS);
    conf->GetValueFatalIfFail("excutorOpt.minRetryTimesForceTimeoutBackoff",
                              &opts->minRetryTimesForceTimeoutBackoff);
    conf->GetValueFatalIfFail("excutorOpt.maxRetryTimesBeforeConsiderSuspend",
                              &opts->maxRetryTimesBeforeConsiderSuspend);
}

void InitSpaceServerOption(Configuration *conf,
                           SpaceAllocServerOption *spaceOpt) {
    conf->GetValueFatalIfFail("spaceserver.spaceaddr",
                              &spaceOpt->spaceaddr);
    conf->GetValueFatalIfFail("spaceserver.rpcTimeoutMs",
                              &spaceOpt->rpcTimeoutMs);
}

void InitBlockDeviceOption(Configuration *conf,
                           BlockDeviceClientOptions *bdevOpt) {
    conf->GetValueFatalIfFail("bdev.confpath", &bdevOpt->configPath);
}

void InitS3Option(Configuration *conf, S3Option *s3Opt) {
    conf->GetValueFatalIfFail("s3.blocksize", &s3Opt->blocksize);
    conf->GetValueFatalIfFail("s3.chunksize", &s3Opt->chunksize);
    ::curve::common::InitS3AdaptorOption(conf, &s3Opt->s3AdaptrOpt);
}

void InitDCacheOption(Configuration *conf, DCacheOption *dcacheOpt) {
    conf->GetValueFatalIfFail("dCache.maxListDentryCount",
                              &dcacheOpt->maxListDentryCount);
}

void InitExtentManagerOption(Configuration *conf,
                             ExtentManagerOption *extentManagerOpt) {
    conf->GetValueFatalIfFail("extentManager.preAllocSize",
                              &extentManagerOpt->preAllocSize);
}

void SetBrpcOpt(Configuration *conf) {
    conf->GetValueFatalIfFail("defer.close.second",
                              &brpc::FLAGS_defer_close_second);
}

void InitFuseClientOption(Configuration *conf, FuseClientOption *clientOption) {
    InitMdsOption(conf, &clientOption->mdsOpt);
    InitMetaCacheOption(conf, &clientOption->metaCacheOpt);
    InitExcutorOption(conf, &clientOption->excutorOpt);
    InitSpaceServerOption(conf, &clientOption->spaceOpt);
    InitBlockDeviceOption(conf, &clientOption->bdevOpt);
    InitS3Option(conf, &clientOption->s3Opt);
    InitDCacheOption(conf, &clientOption->dcacheOpt);
    InitExtentManagerOption(conf, &clientOption->extentManagerOpt);

    conf->GetValueFatalIfFail("fuseClient.attrTimeOut",
                              &clientOption->attrTimeOut);
    conf->GetValueFatalIfFail("fuseClient.entryTimeOut",
                              &clientOption->entryTimeOut);
    conf->GetValueFatalIfFail("fuseClient.bigFileSize",
                              &clientOption->bigFileSize);
    SetBrpcOpt(conf);
}

}  // namespace common
}  // namespace client
}  // namespace curvefs

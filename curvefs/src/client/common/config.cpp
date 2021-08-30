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

namespace brpc {
DECLARE_int32(defer_close_second);
}  // namespace brpc


namespace curvefs {
namespace client {
namespace common {
void InitMdsOption(Configuration *conf,
    ::curve::client::MetaServerOption *mdsOpt) {
    // TODO(xuchaojie) : load from config file
}

void InitMetaServerOption(Configuration *conf, MetaServerOption *metaOpt) {
    LOG_IF(FATAL, !conf->GetStringValue("metaserver.msaddr", &metaOpt->msaddr));
    LOG_IF(FATAL, !conf->GetUInt64Value("metaserver.rpcTimeoutMs",
                                        &metaOpt->rpcTimeoutMs));
}

void InitSpaceServerOption(Configuration *conf,
                           SpaceAllocServerOption *spaceOpt) {
    LOG_IF(FATAL, !conf->GetStringValue("spaceserver.spaceaddr",
                                        &spaceOpt->spaceaddr));
    LOG_IF(FATAL, !conf->GetUInt64Value("spaceserver.rpcTimeoutMs",
                                        &spaceOpt->rpcTimeoutMs));
}

void InitBlockDeviceOption(Configuration *conf,
                           BlockDeviceClientOptions *bdevOpt) {
    LOG_IF(FATAL, !conf->GetStringValue("bdev.confpath", &bdevOpt->configPath));
}

void InitS3Option(Configuration *conf, S3Option *s3Opt) {
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.blocksize", &s3Opt->blocksize));
    LOG_IF(FATAL, !conf->GetUInt64Value("s3.chunksize", &s3Opt->chunksize));
    ::curve::common::InitS3AdaptorOption(conf, &s3Opt->s3AdaptrOpt);
}

void InitDCacheOption(Configuration *conf, DCacheOption *dcacheOpt) {
    LOG_IF(FATAL, !conf->GetUInt32Value("dCache.maxListDentryCount",
                                        &dcacheOpt->maxListDentryCount));
}

void InitExtentManagerOption(Configuration *conf,
                             ExtentManagerOption *extentManagerOpt) {
    LOG_IF(FATAL, !conf->GetUInt64Value("extentManager.preAllocSize",
                                        &extentManagerOpt->preAllocSize));
}

void SetBrpcOpt(Configuration *conf) {
    LOG_IF(FATAL, !conf->GetIntValue("defer.close.second",
                                     brpc::FLAGS_defer_close_second));
}


void InitFuseClientOption(Configuration *conf, FuseClientOption *clientOption) {
    InitMdsOption(conf, &clientOption->mdsOpt);
    InitMetaServerOption(conf, &clientOption->metaOpt);
    InitSpaceServerOption(conf, &clientOption->spaceOpt);
    InitBlockDeviceOption(conf, &clientOption->bdevOpt);
    InitS3Option(conf, &clientOption->s3Opt);
    InitDCacheOption(conf, &clientOption->dcacheOpt);
    InitExtentManagerOption(conf, &clientOption->extentManagerOpt);

    LOG_IF(FATAL, !conf->GetDoubleValue("fuseClient.attrTimeOut",
                                        &clientOption->attrTimeOut));
    LOG_IF(FATAL, !conf->GetDoubleValue("fuseClient.entryTimeOut",
                                        &clientOption->entryTimeOut));
    LOG_IF(FATAL, !conf->GetUInt64Value("fuseClient.bigFileSize",
                                        &clientOption->bigFileSize));

    SetBrpcOpt(conf);
}

}  // namespace common
}  // namespace client
}  // namespace curvefs

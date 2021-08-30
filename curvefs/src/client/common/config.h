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
struct BlockDeviceClientOptions {
    // config path
    std::string configPath;
};

struct MetaServerOption {
    std::string msaddr;
    uint64_t rpcTimeoutMs;
};

struct SpaceAllocServerOption {
    std::string spaceaddr;
    uint64_t rpcTimeoutMs;
};

struct S3Option {
    uint64_t blocksize;
    uint64_t chunksize;
    S3AdapterOption s3AdaptrOpt;
};

struct DCacheOption {
    uint32_t maxListDentryCount;
};

struct ExtentManagerOption {
    uint64_t preAllocSize;
};

struct FuseClientOption {
    ::curve::client::MetaServerOption mdsOpt;
    ::curvefs::client::common::MetaServerOption metaOpt;
    SpaceAllocServerOption spaceOpt;
    BlockDeviceClientOptions bdevOpt;
    S3Option s3Opt;
    DCacheOption dcacheOpt;
    ExtentManagerOption extentManagerOpt;

    double attrTimeOut;
    double entryTimeOut;
    uint64_t bigFileSize;
};

void InitFuseClientOption(Configuration *conf, FuseClientOption *clientOption);
}  // namespace common
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_COMMON_CONFIG_H_

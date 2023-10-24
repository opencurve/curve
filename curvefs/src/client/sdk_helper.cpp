/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-08-08
 * Author: Jingli Chen (Wine93)
 */

// clang-format off

#include "curvefs/src/client/logger/access_log.h"
#include "curvefs/src/client/logger/error_log.h"
#include "src/common/gflags_helper.h"
#include "curvefs/src/client/sdk_helper.h"

namespace curvefs {
namespace client {

using ::curve::common::GflagsLoadValueFromConfIfCmdNotSet;
using ::curvefs::client::logger::InitAccessLog;
using ::curvefs::client::logger::InitErrorLog;
using ::curvefs::client::logger::ShutdownErrorLog;

void SDKHelper::InitLog(Configuration* cfg) {
    // prefix
    std::string prefix;
    cfg->GetStringValue("client.common.logDir", &prefix);

    // name
    static std::string name = "/curvefs-sdk";

    // log level
    int32_t loglevel = 0;
    curve::common::GflagsLoadValueFromConfIfCmdNotSet dummy;
    dummy.Load(cfg, "v", "client.loglevel", &loglevel);

    InitErrorLog(prefix, name, loglevel);
    InitAccessLog(prefix);
}

void SDKHelper::InitOption(Configuration* cfg, FuseClientOption* option) {
    cfg->PrintConfig();
    InitFuseClientOption(cfg, option);
}

FSType SDKHelper::Str2Type(const std::string& s) {
    if (s == "s3") {
        return FSType::TYPE_S3;
    } else if (s == "volume") {
        return FSType::TYPE_VOLUME;
    }
    return FSType();
}

std::string SDKHelper::Type2Str(FSType t) {
    if (t == FSType::TYPE_S3) {
        return "s3";
    } else if (t == FSType::TYPE_VOLUME) {
        return "volume";
    }
    return "unknown";
}

bool SDKHelper::GetFsInfoFromMDS(const MdsOption& option,
                                 const std::string& fsname,
                                 FsInfo* info) {
    MdsClientImpl client;
    MDSBaseClient base;
    client.Init(option, &base);
    auto rc = client.GetFsInfo(fsname, info);
    if (rc != FSStatusCode::OK) {
        LOG(ERROR) << "Get fsinfo from mds failed, fsname = " << fsname
                   << ", retCode = " << FSStatusCode_Name(rc);
        return false;
    }
    return true;
}

bool SDKHelper::CheckFsType(FSType real, FSType arg) {
    if (real != arg) {
        LOG(ERROR) << "The fstype obtained from the mds is "
                   << FSType_Name(real) << ", but user specified is "
                   << FSType_Name(arg);
        return false;
    } else if (real != FSType::TYPE_VOLUME && real != FSType::TYPE_S3) {
        LOG(ERROR) << "The fstype obtained from the mds is "
                   << FSType_Name(real) << ", which is not supported";
        return false;
    }
    return true;
}

MountOption SDKHelper::GetMountOption(const std::string& fsname,
                                      const std::string& mountpoint) {
    auto mountOption = MountOption();
    mountOption.mountPoint = mountpoint.c_str();
    mountOption.fsName = fsname.c_str();
    mountOption.fsType = Type2Str(FSType::TYPE_S3).c_str();
    return mountOption;
}

CURVEFS_ERROR SDKHelper::CheckMountOption(std::shared_ptr<FuseClient> client,
                                          const MdsOption& mdsOption,
                                          MountOption* mountOption) {
    FsInfo info;
    bool yes = GetFsInfoFromMDS(mdsOption, mountOption->fsName, &info) &&
               CheckFsType(info.fstype(), Str2Type(mountOption->fsType));
    if (!yes) {
        return CURVEFS_ERROR::INTERNAL;
    }
    client->SetFsInfo(std::make_shared<FsInfo>(info));
    return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR SDKHelper::Mount(std::shared_ptr<FuseClient> client,
                               const std::string& fsname,
                               const std::string& mountpoint,
                               FuseClientOption option) {
    auto mountOption = GetMountOption(fsname, mountpoint);
    auto rc = CheckMountOption(client, option.mdsOpt, &mountOption);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = client->Init(option);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = client->Run();
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = client->SetMountStatus(&mountOption);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    return client->FuseOpInit(nullptr, nullptr);
}

CURVEFS_ERROR SDKHelper::Umount(std::shared_ptr<FuseClient> client,
                                const std::string& fsname,
                                const std::string& mountpoint) {
    auto mountOption = GetMountOption(fsname, mountpoint);
    client->FuseOpDestroy(&mountOption);
    client->Fini();
    client->UnInit();
    ShutdownErrorLog();
    return CURVEFS_ERROR::OK;
}

}  // namespace client
}  // namespace curvefs

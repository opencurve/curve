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

#include "src/common/uuid.h"
#include "curvefs/src/client/logger/error_log.h"
#include "curvefs/src/client/logger/access_log.h"
#include "src/common/gflags_helper.h"
#include "curvefs/src/client/helper.h"

namespace curvefs {
namespace client {

using ::curve::common::UUIDGenerator;
using ::curve::common::GflagsLoadValueFromConfIfCmdNotSet;
using ::curvefs::client::logger::InitAccessLog;
using ::curvefs::client::logger::InitErrorLog;
using ::curvefs::client::logger::ShutdownErrLog;

bool Helper::LoadCfg(const std::string& path, Configuration* cfg) {
    cfg->SetConfigPath(path);
    if (!cfg->LoadConfig()) {
        LOG(ERROR) << "Load config from file failed, path = " << path;
        return false;
    }
    return true;
}


CURVEFS_ERROR Helper::InitLog(FuseClientOption option) {
    /*
    cfg->PrintConfig();
    cfg->GetStringValue("client.common.logDir", &prefix);

    // log level
    int32_t loglevel = 0;
    curve::common::GflagsLoadValueFromConfIfCmdNotSet dummy;
    dummy.Load(cfg, "v", "client.loglevel", &loglevel);
    */

    int32_t loglevel = 6;
    std::string prefix = "/tmp";
    InitErrorLog(prefix, "", loglevel);
    return CURVEFS_ERROR::OK;
}

bool Helper::GetFSInfoFromMDS(const MdsOption& option,
                              const std::string& fsname,
                              FsInfo* info) {
    MdsClientImpl client;
    MDSBaseClient base;
    client.Init(option, &base);
    auto rc = client.GetFsInfo(fsname, info);
    if (rc != FSStatusCode::OK) {
        LOG(ERROR) << "Get fsinfo from mds failed, fsname = "
                   << fsname << ", retCode = " << FSStatusCode_Name(rc);
        return false;
    }
    return true;
}

bool Helper::CheckFSType(FSType real, FSType arg) {
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

FSType Helper::Str2Type(const std::string& s) {
    if (s == "s3") {
        return FSType::TYPE_S3;
    } else if (s == "volume") {
        return FSType::TYPE_VOLUME;
    }
    return FSType();
}

std::string Helper::Type2Str(FSType t) {
    if (t == FSType::TYPE_S3) {
        return "s3";
    } else if (t == FSType::TYPE_VOLUME) {
        return "volume";
    }
    return "unknown";
}

CURVEFS_ERROR Helper::Precheck(std::shared_ptr<FuseClient> client,
                               struct MountOption* mount,
                               FuseClientOption option) {
    FsInfo info;
    bool yes = GetFSInfoFromMDS(option.mdsOpt, mount->fsName, &info) &&
               CheckFSType(info.fstype(), Str2Type(mount->fsType));
    if (yes) {
        client->SetFsInfo(std::make_shared<FsInfo>(info));
        return CURVEFS_ERROR::OK;
    }
    return CURVEFS_ERROR::INTERNAL;
}

void Helper::InitOption(Configuration* cfg, FuseClientOption* option) {
    InitErrorLog("/tmp", "", 0);
    InitFuseClientOption(cfg, option);
}

struct MountOption* Helper::GetMount(const std::string& fsname,
                                     const std::string& mountpoin) {
    static struct MountOption mount;
    auto uuid = UUIDGenerator().GenerateUUID();  // FIXME: mountpoint
    mount.mountPoint = new char[500];
    mount.fsName = new char[100];
    mount.fsType = new char[100];

    strcpy(mount.mountPoint, uuid.c_str());
    strcpy(mount.fsName, fsname.c_str());
    strcpy(mount.fsType, Type2Str(FSType::TYPE_S3).c_str());
    return &mount;
}

CURVEFS_ERROR Helper::Mount(std::shared_ptr<FuseClient> client,
                            const std::string& fsname,
                            const std::string& mountpoin,
                            FuseClientOption option) {
    auto mount = GetMount(fsname, mountpoin);
    auto rc = InitLog(option);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = Precheck(client, mount, option);
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = client->Init(option);
    if (rc == CURVEFS_ERROR::OK) {
        rc = client->Run();
    }
    if (rc != CURVEFS_ERROR::OK) {
        return rc;
    }

    rc = client->SetMountStatus(mount);
    if (rc == CURVEFS_ERROR::OK) {
        rc = client->FuseOpInit(nullptr, nullptr);
    }
    return rc;
}

CURVEFS_ERROR Helper::Umount(std::shared_ptr<FuseClient> client) {
    client->FuseOpDestroy(nullptr);
    client->Fini();
    client->UnInit();
    ShutdownErrLog();
    return CURVEFS_ERROR::OK;
}

}  // namespace client
}  // namespace curvefs

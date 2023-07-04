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

#include "curvefs/src/client/logger/error_log.h"
#include "curvefs/src/client/logger/access_log.h"
#include "src/common/gflags_helper.h"
#include "curvefs/src/client/helper.h"

namespace curvefs {
namespace client {

using ::curve::common::GflagsLoadValueFromConfIfCmdNotSet;
using ::curvefs::client::logger::InitAccessLog;
using ::curvefs::client::logger::InitErrorLog;

bool Helper::LoadCfg(const std::string& path, Configuration* cfg) {
    cfg->SetConfigPath(path);
    if (!cfg->LoadConfig()) {
        LOG(ERROR) << "Load config from file failed, path = " << path;
        return false;
    }
    return true;
}

void Helper::InitOption(Configuration* cfg, FuseClientOption* option) {
    InitFuseClientOption(cfg, option);
}

void Helper::RewriteMDSAddr(Configuration* cfg, const char* addr) {
    if (addr != nullptr) {
        cfg->SetStringValue("mdsOpt.rpcRetryOpt.addrs", addr);
    }
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

std::shared_ptr<FuseClient> Helper::NewClient(FsInfo info) {
    std::shared_ptr<FuseClient> client;
    auto type = info.fstype();
    if (type == FSType::TYPE_S3) {
        client = std::make_shared<FuseS3Client>();
    } else if (type == FSType::TYPE_VOLUME) {
        client = std::make_shared<FuseVolumeClient>();
    }
    client->SetFsInfo(std::make_shared<FsInfo>(info));
    return client;
}

bool Helper::RunClient(std::shared_ptr<FuseClient> client,
                       FuseClientOption option) {
    auto rc = client->Run();
    if (rc != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "Run client failed, retCode = " << rc;
        return false;
    }
    return true;
}

bool Helper::Mount(std::shared_ptr<FuseClient> client,
                   const MountOption* mount) {
    auto rc = client->SetMountStatus(mount);
    if (rc != CURVEFS_ERROR::OK) {
        return false;
    }
    return true;
}

bool Helper::InitLog(Configuration* cfg) {
    //cfg->PrintConfig();

    // log dir
    std::string prefix = "/tmp";
    cfg->GetStringValue("client.common.logDir", &prefix);

    // log level
    int32_t loglevel = 0;
    curve::common::GflagsLoadValueFromConfIfCmdNotSet dummy;
    dummy.Load(cfg, "v", "client.loglevel", &loglevel);

    // FIXME(Wine93): name
    bool yes = InitErrorLog(prefix, "", loglevel);
    if (yes) {
        yes = InitAccessLog(prefix);
    }

    return yes;
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

bool Helper::NewClient(const MountOption* mount,
                       Configuration* cfg,
                       std::shared_ptr<FuseClient>* client) {
    // 1) init log
    bool yes = InitLog(cfg);
    if (!yes) {
        return false;
    }

    // 2) get fsinfo from mds
    FuseClientOption option;
    InitOption(cfg, &option);
    FsInfo info;
    yes = GetFSInfoFromMDS(option.mdsOpt, mount->fsName, &info);
    if (!yes) {
        return false;
    }

    // 3) check fstype
    yes = CheckFSType(info.fstype(), Str2Type(mount->fsType));
    if (!yes) {
        return false;
    }

    // 4) new client and run it
    *client = NewClient(info);
    auto rc = (*client)->Init(option);
    if (rc != CURVEFS_ERROR::OK) {
        return false;
    }
    rc = (*client)->Run();
    if (rc != CURVEFS_ERROR::OK) {
        return false;
    }

    // 5) mount to mds
    yes = Mount(*client, mount);
    if (!yes) {
        return false;
    }

    rc = (*client)->FuseOpInit(nullptr, nullptr);
    if (rc != CURVEFS_ERROR::OK) {
        return false;
    }

    return true;
}

bool Helper::NewClientForFuse(const MountOption* mount,
                              std::shared_ptr<FuseClient>* client) {
    Configuration cfg;
    bool yes = LoadCfg(mount->conf, &cfg);
    if (!yes) {
        return false;
    }
    RewriteMDSAddr(&cfg, mount->mdsAddr);
    return NewClient(mount, &cfg, client);
}

bool Helper::NewClientForSDK(const std::string& fsname,
                             const std::string& mountpoint,
                             Configuration* cfg,
                             std::shared_ptr<FuseClient>* client) {
    struct MountOption mount;
    mount.mountPoint = mountpoint.c_str();
    mount.fsName = fsname.c_str();
    mount.fsType = Type2Str(FSType::TYPE_S3).c_str();
    bool yes = NewClient(&mount, cfg, client);
    return yes;
}

}  // namespace client
}  // namespace curvefs

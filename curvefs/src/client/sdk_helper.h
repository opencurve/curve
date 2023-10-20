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

#ifndef CURVEFS_SRC_CLIENT_SDK_HELPER_H_
#define CURVEFS_SRC_CLIENT_SDK_HELPER_H_

#include <memory>
#include <string>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/fuse_client.h"
#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/fuse_s3_client.h"
#include "curvefs/src/client/fuse_volume_client.h"
#include "src/common/configuration.h"

namespace curvefs {
namespace client {

using ::curve::common::Configuration;
using ::curvefs::client::common::MdsOption;
using ::curvefs::common::FSType;
using ::curvefs::mds::FsInfo;

class SDKHelper {
 public:
    SDKHelper() = default;

    void InitLog(Configuration* cfg);

    void InitOption(Configuration* cfg, FuseClientOption* option);

    CURVEFS_ERROR Mount(std::shared_ptr<FuseClient> client,
                        const std::string& fsname,
                        const std::string& mountpoint,
                        FuseClientOption option);

    CURVEFS_ERROR Umount(std::shared_ptr<FuseClient> client,
                         const std::string& fsname,
                         const std::string& mountpoint);

 private:
    FSType Str2Type(const std::string& s);

    std::string Type2Str(FSType t);

    bool GetFsInfoFromMDS(const MdsOption& option,
                          const std::string& fsname,
                          FsInfo* info);

    bool CheckFsType(FSType real, FSType arg);

    MountOption GetMountOption(const std::string& fsname,
                               const std::string& mountpoint);

    CURVEFS_ERROR CheckMountOption(std::shared_ptr<FuseClient> client,
                                   const MdsOption& mdsOption,
                                   MountOption* mountOption);
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_SDK_HELPER_H_

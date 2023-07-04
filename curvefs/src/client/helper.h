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

#ifndef CURVEFS_SRC_CLIENT_HELPER_H_
#define CURVEFS_SRC_CLIENT_HELPER_H_

#include <string>
#include <memory>

#include "src/common/configuration.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/client/fuse_client.h"
#include "curvefs/src/client/fuse_s3_client.h"
#include "curvefs/src/client/fuse_volume_client.h"
#include "curvefs/src/client/fuse_common.h"
#include "curvefs/src/client/common/config.h"

namespace curvefs {
namespace client {

using ::curve::common::Configuration;
using ::curvefs::common::FSType;
using ::curvefs::mds::FsInfo;
using ::curvefs::client::common::MdsOption;

class Helper {
 public:
    Helper() = default;

    bool NewClientForFuse(const MountOption* option,
                          std::shared_ptr<FuseClient>* client);

    bool NewClientForSDK(const std::string& fsname,
                         const std::string& mountpoint,
                         Configuration* cfg,
                         std::shared_ptr<FuseClient>* client);

 private:
    bool LoadCfg(const std::string& path, Configuration* cfg);

    void InitOption(Configuration* cfg, FuseClientOption* option);

    void RewriteMDSAddr(Configuration* cfg, const char* addr);

    bool GetFSInfoFromMDS(const MdsOption& option,
                          const std::string& fsname,
                          FsInfo* info);

    bool CheckFSType(FSType real, FSType arg);

    std::shared_ptr<FuseClient> NewClient(FsInfo info);

    bool RunClient(std::shared_ptr<FuseClient> client, FuseClientOption option);

    bool Mount(std::shared_ptr<FuseClient> client, const MountOption* option);

    bool InitLog(Configuration* cfg);

    bool NewClient(const MountOption* mount,
                   Configuration* cfg,
                   std::shared_ptr<FuseClient>* client);

    FSType Str2Type(const std::string& s);

    std::string Type2Str(FSType t);
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_HELPER_H_

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

#ifndef CURVEFS_SRC_CLIENT_MDS_CLIENT_H_
#define CURVEFS_SRC_CLIENT_MDS_CLIENT_H_

#include <string>

#include "src/client/mds_client.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/client/config.h"
#include "curvefs/src/client/error_code.h"

using ::curvefs::mds::FsInfo;
using ::curve::client::MDSClient;

namespace curvefs {
namespace client {

class MdsClient {
 public:
    MdsClient() {}
    virtual ~MdsClient() {}

    virtual CURVEFS_ERROR CreateFs(const std::string &fsName,
        const std::string &volumeName) = 0;

    virtual CURVEFS_ERROR DeleteFs(const std::string &fsName) = 0;

    virtual CURVEFS_ERROR MountFs(
        const std::string &fsName, const std::string& mountPoint) = 0;

    virtual CURVEFS_ERROR UmountFs(
        const std::string &fsName, const std::string& mountPoint) = 0;

    virtual CURVEFS_ERROR GetFsInfo(
        const std::string& fsName, FsInfo* fsInfo) = 0;

    virtual CURVEFS_ERROR GetFsInfo(uint32_t fsId, FsInfo* fsInfo) = 0;
};

class MdsClientImpl : public MdsClient {
 public:
    MdsClientImpl() {}

    CURVEFS_ERROR Init(const MdsOption& mdsOpt);

    CURVEFS_ERROR CreateFs(const std::string &fsName,
        const std::string &volumeName) override;

    CURVEFS_ERROR DeleteFs(const std::string &fsName) override;

    CURVEFS_ERROR MountFs(
        const std::string &fsName, const std::string& mountPoint) override;

    CURVEFS_ERROR UmountFs(
        const std::string &fsName, const std::string& mountPoint) override;

    CURVEFS_ERROR GetFsInfo(
        const std::string& fsName, FsInfo* fsInfo) override;

    CURVEFS_ERROR GetFsInfo(uint32_t fsId, FsInfo* fsInfo) override;

 private:
    MDSClient client_;
};

}  // namespace client
}  // namespace curvefs


#endif  // CURVEFS_SRC_CLIENT_MDS_CLIENT_H_

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
 * Created Date: Thur Jun 15 2021
 * Author: lixiaocui
 */

#include "curvefs/src/client/rpcclient/mds_client.h"

namespace curvefs {
namespace client {
namespace rpcclient {

FSStatusCode
MdsClientImpl::Init(const ::curve::client::MetaServerOption &mdsOpt,
                    MDSBaseClient *baseclient) {
    return FSStatusCode::OK;
}


FSStatusCode MdsClientImpl::CreateFs(const std::string &fsName,
                                     uint64_t blockSize, const Volume &volume) {
    return FSStatusCode::OK;
}


FSStatusCode MdsClientImpl::CreateFsS3(const std::string &fsName,
                                       uint64_t blockSize,
                                       const S3Info &s3Info) {
    return FSStatusCode::OK;
}


FSStatusCode MdsClientImpl::DeleteFs(const std::string &fsName) {
    return FSStatusCode::OK;
}


FSStatusCode MdsClientImpl::MountFs(const std::string &fsName,
                                    const std::string &mountPt,
                                    FsInfo *fsInfo) {
    return FSStatusCode::OK;
}

FSStatusCode MdsClientImpl::UmountFs(const std::string &fsName,
                                     const std::string &mountPt) {
    return FSStatusCode::OK;
}

FSStatusCode MdsClientImpl::GetFsInfo(const std::string &fsName,
                                      FsInfo *fsInfo) {
    return FSStatusCode::OK;
}

FSStatusCode MdsClientImpl::GetFsInfo(uint32_t fsId, FsInfo *fsInfo) {
    return FSStatusCode::OK;
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

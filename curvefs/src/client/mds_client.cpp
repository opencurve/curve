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

#include "curvefs/src/client/mds_client.h"

namespace curvefs {
namespace client {

CURVEFS_ERROR MdsClientImpl::Init(const MdsOption& mdsOpt) {
    return CURVEFS_ERROR::OK; }

CURVEFS_ERROR MdsClientImpl::CreateFs(const std::string &fsName,
    const std::string &volumeName) { return CURVEFS_ERROR::OK; }

CURVEFS_ERROR MdsClientImpl::DeleteFs(const std::string &fsName) {
    return CURVEFS_ERROR::OK; }

CURVEFS_ERROR MdsClientImpl::MountFs(
    const std::string &fsName, const std::string& mountPoint) {
    return CURVEFS_ERROR::OK; }

CURVEFS_ERROR MdsClientImpl::UmountFs(
    const std::string &fsName, const std::string& mountPoint) {
    return CURVEFS_ERROR::OK; }

CURVEFS_ERROR MdsClientImpl::GetFsInfo(
    const std::string& fsName, FsInfo* fsInfo) { return CURVEFS_ERROR::OK; }

CURVEFS_ERROR MdsClientImpl::GetFsInfo(
    uint32_t fsId, FsInfo* fsInfo) { return CURVEFS_ERROR::OK; }

}  // namespace client
}  // namespace curvefs

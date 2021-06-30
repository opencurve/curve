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
 * Created Date: Fri Jun 11 2021
 * Author: lixiaocui
 */
#ifndef CURVEFS_SRC_CLIENT_BASE_CLIENT_H_
#define CURVEFS_SRC_CLIENT_BASE_CLIENT_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <list>
#include <string>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/client/extent.h"

namespace curvefs {
namespace client {

using curvefs::metaserver::CreateDentryRequest;
using curvefs::metaserver::CreateDentryResponse;
using curvefs::metaserver::CreateInodeRequest;
using curvefs::metaserver::CreateInodeResponse;
using curvefs::metaserver::DeleteDentryRequest;
using curvefs::metaserver::DeleteDentryResponse;
using curvefs::metaserver::DeleteInodeRequest;
using curvefs::metaserver::DeleteInodeResponse;
using curvefs::metaserver::Dentry;
using ::curvefs::metaserver::FsFileType;
using curvefs::metaserver::GetDentryRequest;
using curvefs::metaserver::GetDentryResponse;
using curvefs::metaserver::GetInodeRequest;
using curvefs::metaserver::GetInodeResponse;
using curvefs::metaserver::Inode;
using curvefs::metaserver::ListDentryRequest;
using curvefs::metaserver::ListDentryResponse;
using curvefs::metaserver::UpdateInodeRequest;
using curvefs::metaserver::UpdateInodeResponse;

using curvefs::common::Volume;
using curvefs::mds::CreateFsRequest;
using curvefs::mds::CreateFsResponse;
using curvefs::mds::DeleteFsRequest;
using curvefs::mds::DeleteFsResponse;
using curvefs::mds::FsInfo;
using curvefs::mds::FSType;
using curvefs::mds::FsStatus;
using curvefs::mds::GetFsInfoRequest;
using curvefs::mds::GetFsInfoResponse;
using curvefs::mds::MountFsRequest;
using curvefs::mds::MountFsResponse;
using curvefs::mds::UmountFsRequest;
using curvefs::mds::UmountFsResponse;

using curvefs::space::AllocateSpaceRequest;
using curvefs::space::AllocateSpaceResponse;
using curvefs::space::DeallocateSpaceRequest;
using curvefs::space::DeallocateSpaceResponse;
using curvefs::space::Extent;


struct InodeParam {
    uint64_t fsId;
    uint64_t length;
    uint32_t uid;
    uint32_t gid;
    uint32_t mode;
    FsFileType type;
    std::string symlink;
};


class MetaServerBaseClient {
 public:
    MetaServerBaseClient() {}

    virtual ~MetaServerBaseClient() {}

    virtual void GetDentry(uint32_t fsId, uint64_t inodeid,
                           const std::string &name, GetDentryResponse *response,
                           brpc::Controller *cntl, brpc::Channel *channel);

    virtual void ListDentry(uint32_t fsId, uint64_t inodeid,
                            const std::string &last, uint32_t count,
                            ListDentryResponse *response,
                            brpc::Controller *cntl, brpc::Channel *channel);

    virtual void CreateDentry(const Dentry &dentry,
                              CreateDentryResponse *response,
                              brpc::Controller *cntl, brpc::Channel *channel);

    virtual void DeleteDentry(uint32_t fsId, uint64_t inodeid,
                              const std::string &name,
                              DeleteDentryResponse *response,
                              brpc::Controller *cntl, brpc::Channel *channel);

    virtual void GetInode(uint32_t fsId, uint64_t inodeid,
                          GetInodeResponse *response, brpc::Controller *cntl,
                          brpc::Channel *channel);

    virtual void UpdateInode(const Inode &inode, UpdateInodeResponse *response,
                             brpc::Controller *cntl, brpc::Channel *channel);

    virtual void CreateInode(const InodeParam &param,
                             CreateInodeResponse *response,
                             brpc::Controller *cntl, brpc::Channel *channel);

    virtual void DeleteInode(uint32_t fsId, uint64_t inodeid,
                             DeleteInodeResponse *response,
                             brpc::Controller *cntl, brpc::Channel *channel);
};

class MDSBaseClient {
 public:
    virtual void CreateFs(const std::string &fsName, uint64_t blockSize,
                          const Volume &volume, CreateFsResponse *response,
                          brpc::Controller *cntl, brpc::Channel *channel);

    virtual void DeleteFs(const std::string &fsName, DeleteFsResponse *response,
                          brpc::Controller *cntl, brpc::Channel *channel);

    virtual void MountFs(const std::string &fsName, const std::string &mountPt,
                         MountFsResponse *response, brpc::Controller *cntl,
                         brpc::Channel *channel);

    virtual void UmountFs(const std::string &fsName, const std::string &mountPt,
                          UmountFsResponse *response, brpc::Controller *cntl,
                          brpc::Channel *channel);

    virtual void GetFsInfo(const std::string &fsName,
                           GetFsInfoResponse *response, brpc::Controller *cntl,
                           brpc::Channel *channel);

    virtual void GetFsInfo(uint32_t fsId, GetFsInfoResponse *response,
                           brpc::Controller *cntl, brpc::Channel *channel);
};

class SpaceBaseClient {
 public:
    virtual void AllocExtents(uint32_t fsId,
                              const ExtentAllocInfo &toAllocExtent,
                              curvefs::space::AllocateType type,
                              AllocateSpaceResponse *response,
                              brpc::Controller *cntl, brpc::Channel *channel);

    virtual void DeAllocExtents(uint32_t fsId,
                                const std::list<Extent> &allocatedExtents,
                                DeallocateSpaceResponse *response,
                                brpc::Controller *cntl, brpc::Channel *channel);
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BASE_CLIENT_H_

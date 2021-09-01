/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"){}
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

#include "curvefs/src/client/rpcclient/base_client.h"
#include "curvefs/src/client/common/extent.h"

namespace curvefs {
namespace client {
namespace rpcclient {

using curvefs::metaserver::VolumeExtentList;

void MetaServerBaseClient::GetDentry(uint32_t fsId, uint64_t inodeid,
                                     const std::string &name,
                                     GetDentryResponse *response,
                                     brpc::Controller *cntl,
                                     brpc::Channel *channel) {
    GetDentryRequest request;
    request.set_fsid(fsId);
    request.set_parentinodeid(inodeid);
    request.set_name(name);

    curvefs::metaserver::MetaServerService_Stub stub(channel);
    stub.GetDentry(cntl, &request, response, nullptr);
}

void MetaServerBaseClient::ListDentry(uint32_t fsId, uint64_t inodeid,
                                      const std::string &last, uint32_t count,
                                      ListDentryResponse *response,
                                      brpc::Controller *cntl,
                                      brpc::Channel *channel) {
    ListDentryRequest request;
    request.set_fsid(fsId);
    request.set_dirinodeid(inodeid);
    request.set_last(last);
    request.set_count(count);

    curvefs::metaserver::MetaServerService_Stub stub(channel);
    stub.ListDentry(cntl, &request, response, nullptr);
}

void MetaServerBaseClient::CreateDentry(const Dentry &dentry,
                                        CreateDentryResponse *response,
                                        brpc::Controller *cntl,
                                        brpc::Channel *channel) {
    CreateDentryRequest request;
    Dentry *d = new Dentry;
    d->set_fsid(dentry.fsid());
    d->set_inodeid(dentry.inodeid());
    d->set_parentinodeid(dentry.parentinodeid());
    d->set_name(dentry.name());
    request.set_allocated_dentry(d);
    curvefs::metaserver::MetaServerService_Stub stub(channel);
    stub.CreateDentry(cntl, &request, response, nullptr);
}

void MetaServerBaseClient::DeleteDentry(uint32_t fsId, uint64_t inodeid,
                                        const std::string &name,
                                        DeleteDentryResponse *response,
                                        brpc::Controller *cntl,
                                        brpc::Channel *channel) {
    DeleteDentryRequest request;
    request.set_fsid(fsId);
    request.set_parentinodeid(inodeid);
    request.set_name(name);
    curvefs::metaserver::MetaServerService_Stub stub(channel);
    stub.DeleteDentry(cntl, &request, response, nullptr);
}

void MetaServerBaseClient::GetInode(uint32_t fsId, uint64_t inodeid,
                                    GetInodeResponse *response,
                                    brpc::Controller *cntl,
                                    brpc::Channel *channel) {
    GetInodeRequest request;
    request.set_fsid(fsId);
    request.set_inodeid(inodeid);
    curvefs::metaserver::MetaServerService_Stub stub(channel);
    stub.GetInode(cntl, &request, response, nullptr);
}

void MetaServerBaseClient::UpdateInode(const Inode &inode,
                                       UpdateInodeResponse *response,
                                       brpc::Controller *cntl,
                                       brpc::Channel *channel) {
    UpdateInodeRequest request;
    request.set_inodeid(inode.inodeid());
    request.set_fsid(inode.fsid());
    request.set_length(inode.length());
    request.set_ctime(inode.ctime());
    request.set_mtime(inode.mtime());
    request.set_atime(inode.atime());
    request.set_uid(inode.uid());
    request.set_gid(inode.gid());
    request.set_mode(inode.mode());
    if (inode.has_volumeextentlist()) {
        VolumeExtentList *vlist = new VolumeExtentList;
        vlist->CopyFrom(inode.volumeextentlist());
        request.set_allocated_volumeextentlist(vlist);
    }
    curvefs::metaserver::MetaServerService_Stub stub(channel);
    stub.UpdateInode(cntl, &request, response, nullptr);
}

void MetaServerBaseClient::CreateInode(const InodeParam &param,
                                       CreateInodeResponse *response,
                                       brpc::Controller *cntl,
                                       brpc::Channel *channel) {
    CreateInodeRequest request;
    request.set_fsid(param.fsId);
    request.set_length(param.length);
    request.set_uid(param.uid);
    request.set_gid(param.gid);
    request.set_mode(param.mode);
    request.set_type(param.type);
    request.set_symlink(param.symlink);
    curvefs::metaserver::MetaServerService_Stub stub(channel);
    stub.CreateInode(cntl, &request, response, nullptr);
}

void MetaServerBaseClient::DeleteInode(uint32_t fsId, uint64_t inodeid,
                                       DeleteInodeResponse *response,
                                       brpc::Controller *cntl,
                                       brpc::Channel *channel) {
    DeleteInodeRequest request;
    request.set_fsid(fsId);
    request.set_inodeid(inodeid);
    curvefs::metaserver::MetaServerService_Stub stub(channel);
    stub.DeleteInode(cntl, &request, response, nullptr);
}


void MDSBaseClient::CreateFs(const std::string &fsName, uint64_t blockSize,
                             const Volume &volume, CreateFsResponse *response,
                             brpc::Controller *cntl, brpc::Channel *channel) {
    CreateFsRequest request;
    request.set_fsname(fsName);
    request.set_blocksize(blockSize);
    request.set_fstype(::curvefs::common::FSType::TYPE_VOLUME);
    Volume *vol = new Volume;
    vol->CopyFrom(volume);
    request.set_allocated_volume(vol);
    curvefs::mds::MdsService_Stub stub(channel);
    stub.CreateFs(cntl, &request, response, nullptr);
}

void MDSBaseClient::CreateFsS3(const std::string &fsName, uint64_t blockSize,
                               const S3Info &s3Info, CreateFsResponse *response,
                               brpc::Controller *cntl, brpc::Channel *channel) {
    CreateFsRequest request;
    request.set_fsname(fsName);
    request.set_blocksize(blockSize);
    request.set_fstype(FSType::TYPE_S3);
    S3Info *info = new S3Info;
    info->CopyFrom(s3Info);
    request.set_allocated_s3info(info);
    curvefs::mds::MdsService_Stub stub(channel);
    stub.CreateFs(cntl, &request, response, nullptr);
}

void MDSBaseClient::DeleteFs(const std::string &fsName,
                             DeleteFsResponse *response, brpc::Controller *cntl,
                             brpc::Channel *channel) {
    DeleteFsRequest request;
    request.set_fsname(fsName);
    curvefs::mds::MdsService_Stub stub(channel);
    stub.DeleteFs(cntl, &request, response, nullptr);
}

void MDSBaseClient::MountFs(const std::string &fsName,
                            const std::string &mountPt,
                            MountFsResponse *response, brpc::Controller *cntl,
                            brpc::Channel *channel) {
    MountFsRequest request;
    request.set_fsname(fsName);
    request.set_mountpoint(mountPt);
    curvefs::mds::MdsService_Stub stub(channel);
    stub.MountFs(cntl, &request, response, nullptr);
}

void MDSBaseClient::UmountFs(const std::string &fsName,
                             const std::string &mountPt,
                             UmountFsResponse *response, brpc::Controller *cntl,
                             brpc::Channel *channel) {
    UmountFsRequest request;
    request.set_fsname(fsName);
    request.set_mountpoint(mountPt);
    curvefs::mds::MdsService_Stub stub(channel);
    stub.UmountFs(cntl, &request, response, nullptr);
}

void MDSBaseClient::GetFsInfo(const std::string &fsName,
                              GetFsInfoResponse *response,
                              brpc::Controller *cntl, brpc::Channel *channel) {
    GetFsInfoRequest request;
    request.set_fsname(fsName);
    curvefs::mds::MdsService_Stub stub(channel);
    stub.GetFsInfo(cntl, &request, response, nullptr);
}

void MDSBaseClient::GetFsInfo(uint32_t fsId, GetFsInfoResponse *response,
                              brpc::Controller *cntl, brpc::Channel *channel) {
    GetFsInfoRequest request;
    request.set_fsid(fsId);
    curvefs::mds::MdsService_Stub stub(channel);
    stub.GetFsInfo(cntl, &request, response, nullptr);
}

void SpaceBaseClient::AllocExtents(uint32_t fsId,
                                   const ExtentAllocInfo &toAllocExtent,
                                   curvefs::space::AllocateType type,
                                   AllocateSpaceResponse *response,
                                   brpc::Controller *cntl,
                                   brpc::Channel *channel) {
    AllocateSpaceRequest request;
    request.set_fsid(fsId);
    request.set_size(toAllocExtent.len);
    auto allochint = new curvefs::space::AllocateHint();
    allochint->set_alloctype(type);
    if (toAllocExtent.leftHintAvailable) {
        allochint->set_leftoffset(toAllocExtent.pOffsetLeft);
    }
    if (toAllocExtent.rightHintAvailable) {
        allochint->set_rightoffset(toAllocExtent.pOffsetRight);
    }
    request.set_allocated_allochint(allochint);
    curvefs::space::SpaceAllocService_Stub stub(channel);
    stub.AllocateSpace(cntl, &request, response, nullptr);
}

void SpaceBaseClient::DeAllocExtents(uint32_t fsId,
                                     const std::list<Extent> &allocatedExtents,
                                     DeallocateSpaceResponse *response,
                                     brpc::Controller *cntl,
                                     brpc::Channel *channel) {
    DeallocateSpaceRequest request;
    request.set_fsid(fsId);
    auto iter = allocatedExtents.begin();
    while (iter != allocatedExtents.end()) {
        auto extent = request.add_extents();
        extent->CopyFrom(*iter);
        ++iter;
    }
    curvefs::space::SpaceAllocService_Stub stub(channel);
    stub.DeallocateSpace(cntl, &request, response, nullptr);
}
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

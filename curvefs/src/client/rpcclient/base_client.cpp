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
void MDSBaseClient::CreateFs(const std::string &fsName, uint64_t blockSize,
                             const Volume &volume, CreateFsResponse *response,
                             brpc::Controller *cntl, brpc::Channel *channel) {
    CreateFsRequest request;
    request.set_fsname(fsName);
    request.set_blocksize(blockSize);
    request.set_fstype(::curvefs::common::FSType::TYPE_VOLUME);
    Volume *vol = new Volume;
    vol->CopyFrom(volume);
    request.mutable_fsdetail()->set_allocated_volume(vol);
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
    request.mutable_fsdetail()->set_allocated_s3info(info);
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

void MDSBaseClient::GetMetaServerInfo(uint32_t port, std::string ip,
                                      GetMetaServerInfoResponse *response,
                                      brpc::Controller *cntl,
                                      brpc::Channel *channel) {
    GetMetaServerInfoRequest request;
    request.set_hostip(ip);
    request.set_port(port);

    curvefs::mds::topology::TopologyService_Stub stub(channel);
    stub.GetMetaServer(cntl, &request, response, nullptr);
}

void MDSBaseClient::GetMetaServerListInCopysets(
    const LogicPoolID &logicalpooid, const std::vector<CopysetID> &copysetidvec,
    GetMetaServerListInCopySetsResponse *response, brpc::Controller *cntl,
    brpc::Channel *channel) {
    GetMetaServerListInCopySetsRequest request;
    request.set_poolid(logicalpooid);
    for (auto copysetid : copysetidvec) {
        request.add_copysetid(copysetid);
    }

    curvefs::mds::topology::TopologyService_Stub stub(channel);
    stub.GetMetaServerListInCopysets(cntl, &request, response, nullptr);
}

void MDSBaseClient::CreatePartition(uint32_t fsID, uint32_t count,
                                    CreatePartitionResponse *response,
                                    brpc::Controller *cntl,
                                    brpc::Channel *channel) {
    CreatePartitionRequest request;
    request.set_fsid(fsID);
    request.set_count(count);

    curvefs::mds::topology::TopologyService_Stub stub(channel);
    stub.CreatePartition(cntl, &request, response, nullptr);
}

void MDSBaseClient::GetCopysetOfPartitions(
    const std::vector<uint32_t> &partitionIDList,
    GetCopysetOfPartitionResponse *response, brpc::Controller *cntl,
    brpc::Channel *channel) {
    GetCopysetOfPartitionRequest request;
    for (auto partitionId : partitionIDList) {
        request.add_partitionid(partitionId);
    }

    curvefs::mds::topology::TopologyService_Stub stub(channel);
    stub.GetCopysetOfPartition(cntl, &request, response, nullptr);
}

void MDSBaseClient::ListPartition(uint32_t fsID,
                                  ListPartitionResponse *response,
                                  brpc::Controller *cntl,
                                  brpc::Channel *channel) {
    ListPartitionRequest request;
    request.set_fsid(fsID);

    curvefs::mds::topology::TopologyService_Stub stub(channel);
    stub.ListPartition(cntl, &request, response, nullptr);
}
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

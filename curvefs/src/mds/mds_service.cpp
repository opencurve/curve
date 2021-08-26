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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include <vector>

#include "curvefs/src/mds/mds_service.h"

namespace curvefs {
namespace mds {
void MdsServiceImpl::CreateFs(::google::protobuf::RpcController* controller,
                              const ::curvefs::mds::CreateFsRequest* request,
                              ::curvefs::mds::CreateFsResponse* response,
                              ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    std::string fsName = request->fsname();
    uint64_t blockSize = request->blocksize();
    FSType type = request->fstype();
    if (type == ::curvefs::common::FSType::TYPE_VOLUME) {
        if (!request->has_volume()) {
            response->set_statuscode(FSStatusCode::PARAM_ERROR);
            LOG(ERROR) << "CreateFs request, type is volume, but has no volume"
                       << ", fsName = " << fsName;
            return;
        }
        curvefs::common::Volume volume = request->volume();
        LOG(INFO) << "CreateFs request, fsName = " << fsName
                  << ", blockSize = " << blockSize
                  << ", volume.volumeName = " << volume.volumename();
        FSStatusCode status = fsManager_->CreateFs(fsName, blockSize, volume,
                                                   response->mutable_fsinfo());
        if (status != FSStatusCode::OK) {
            response->clear_fsinfo();
            response->set_statuscode(status);
            LOG(ERROR) << "CreateFs fail, fsName = " << fsName
                       << ", blockSize = " << blockSize
                       << ", volume.volumeName = " << volume.volumename()
                       << ", errCode = " << FSStatusCode_Name(status);
            return;
        }
    } else if (type == FSType::TYPE_S3) {
        if (!request->has_s3info()) {
            response->set_statuscode(FSStatusCode::PARAM_ERROR);
            LOG(ERROR) << "CreateFs request, type is s3, but has no s3info"
                       << ", fsName = " << fsName;
            return;
        }
        curvefs::common::S3Info s3Info = request->s3info();
        LOG(INFO) << "CreateFs request, fsName = " << fsName
                  << ", blockSize = " << blockSize
                  << ", s3Info.bucketname = " << s3Info.bucketname();
        FSStatusCode status = fsManager_->CreateFs(fsName, blockSize, s3Info,
                                                   response->mutable_fsinfo());
        if (status != FSStatusCode::OK) {
            response->clear_fsinfo();
            response->set_statuscode(status);
            LOG(ERROR) << "CreateFs fail, fsName = " << fsName
                       << ", blockSize = " << blockSize
                       << ", s3Info.bucketname = " << s3Info.bucketname()
                       << ", errCode = " << FSStatusCode_Name(status);
            return;
        }
    } else {
        response->set_statuscode(FSStatusCode::PARAM_ERROR);
        LOG(ERROR) << "CreateFs fail, fs type is invalid"
                   << ", fsName = " << fsName << ", blockSize = " << blockSize
                   << ", fsType = " << type << ", errCode = "
                   << FSStatusCode_Name(FSStatusCode::PARAM_ERROR);
        return;
    }

    response->set_statuscode(FSStatusCode::OK);
    LOG(INFO) << "CreateFs success, fsName = " << fsName
              << ", blockSize = " << blockSize;
    return;
}

void MdsServiceImpl::MountFs(::google::protobuf::RpcController* controller,
                             const ::curvefs::mds::MountFsRequest* request,
                             ::curvefs::mds::MountFsResponse* response,
                             ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    std::string fsName = request->fsname();
    std::string mount = request->mountpoint();
    LOG(INFO) << "MountFs request, fsName = " << fsName
              << ", mountPoint = " << mount;
    FSStatusCode status =
        fsManager_->MountFs(fsName, mount, response->mutable_fsinfo());
    if (status != FSStatusCode::OK) {
        response->clear_fsinfo();
        response->set_statuscode(status);
        LOG(ERROR) << "MountFs fail, fsName = " << fsName
                   << ", mountPoint = " << mount
                   << ", errCode = " << FSStatusCode_Name(status);
        return;
    }

    response->set_statuscode(FSStatusCode::OK);
    LOG(INFO) << "MountFs success, fsName = " << fsName
              << ", mountPoint = " << mount;
    return;
}

void MdsServiceImpl::UmountFs(::google::protobuf::RpcController* controller,
                              const ::curvefs::mds::UmountFsRequest* request,
                              ::curvefs::mds::UmountFsResponse* response,
                              ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    std::string fsName = request->fsname();
    std::string mount = request->mountpoint();
    LOG(INFO) << "UmountFs request, fsName = " << fsName
              << ", mountPoint = " << mount;
    LOG(INFO) << "UmountFs request, " << request->ShortDebugString();
    FSStatusCode status = fsManager_->UmountFs(fsName, mount);
    if (status != FSStatusCode::OK) {
        response->set_statuscode(status);
        LOG(ERROR) << "UmountFs fail, fsName = " << fsName
                   << ", mountPoint = " << mount
                   << ", errCode = " << FSStatusCode_Name(status);
        return;
    }

    response->set_statuscode(FSStatusCode::OK);
    LOG(INFO) << "UmountFs success, fsName = " << fsName
              << ", mountPoint = " << mount;
    return;
}

void MdsServiceImpl::GetFsInfo(::google::protobuf::RpcController* controller,
                               const ::curvefs::mds::GetFsInfoRequest* request,
                               ::curvefs::mds::GetFsInfoResponse* response,
                               ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    uint32_t fsid;
    if (request->has_fsid()) {
        fsid = request->fsid();
    }

    std::string fsName;
    if (request->has_fsname()) {
        fsName = request->fsname();
    }

    LOG(INFO) << "GetFsInfo request: " << request->ShortDebugString();

    FsInfo* fsInfo = response->mutable_fsinfo();
    FSStatusCode status = FSStatusCode::OK;
    if (request->has_fsid() && request->has_fsname()) {
        status = fsManager_->GetFsInfo(fsName, fsid, fsInfo);
    } else if (!request->has_fsid() && request->has_fsname()) {
        status = fsManager_->GetFsInfo(fsName, fsInfo);
    } else if (request->has_fsid() && !request->has_fsname()) {
        status = fsManager_->GetFsInfo(fsid, fsInfo);
    } else {
        status = FSStatusCode::PARAM_ERROR;
    }

    if (status != FSStatusCode::OK) {
        response->clear_fsinfo();
        response->set_statuscode(status);
        LOG(ERROR) << "GetFsInfo fail, request: " << request->ShortDebugString()
                   << ", errCode = " << FSStatusCode_Name(status);
        return;
    }

    response->set_statuscode(FSStatusCode::OK);
    LOG(INFO) << "GetFsInfo success, response: "
              << response->ShortDebugString();
    return;
}

void MdsServiceImpl::DeleteFs(::google::protobuf::RpcController* controller,
                              const ::curvefs::mds::DeleteFsRequest* request,
                              ::curvefs::mds::DeleteFsResponse* response,
                              ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    std::string fsName = request->fsname();
    LOG(INFO) << "DeleteFs request, fsName = " << fsName;
    FSStatusCode status = fsManager_->DeleteFs(fsName);
    if (status != FSStatusCode::OK) {
        response->set_statuscode(status);
        LOG(ERROR) << "DeleteFs fail, fsName = " << fsName
                   << ", errCode = " << FSStatusCode_Name(status);
        return;
    }

    response->set_statuscode(FSStatusCode::OK);
    LOG(INFO) << "DeleteFs success, fsName = " << fsName;
    return;
}

void MdsServiceImpl::DEFINE_RPC(CommitTx) {
    brpc::ClosureGuard doneGuard(done);

    FSStatusCode rc;
    if (request->partitiontxids_size() == 0) {
        rc = FSStatusCode::PARAM_ERROR;
    } else {
        std::vector<PartitionTxId> txIds{
            request->partitiontxids().begin(),
            request->partitiontxids().end()
        };
        rc = fsManager_->CommitTx(request->fsid(), txIds);
    }

    response->set_statuscode(rc);
}

}  // namespace mds
}  // namespace curvefs

/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Friday Feb 25 17:18:47 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/mds/space/service.h"

#include <brpc/closure_guard.h>

#include <vector>

#include "curvefs/src/mds/space/volume_space.h"

namespace curvefs {
namespace mds {
namespace space {

using brpc::ClosureGuard;

void SpaceServiceImpl::AllocateBlockGroup(
    google::protobuf::RpcController* /*controller*/,
    const AllocateBlockGroupRequest* request,
    AllocateBlockGroupResponse* response,
    google::protobuf::Closure* done) {
    ClosureGuard doneGuard(done);

    auto* space = spaceMgr_->GetVolumeSpace(request->fsid());
    if (!space) {
        LOG(WARNING) << "Volume space not found, fsid: " << request->fsid();
        response->set_status(SpaceErrNotFound);
        return;
    }

    std::vector<BlockGroup> groups;
    auto err =
        space->AllocateBlockGroups(request->count(), request->owner(), &groups);
    if (err != SpaceOk) {
        LOG(ERROR) << "Allocate block groups failed, err: "
                   << SpaceErrCode_Name(err);
    } else {
        for (auto& group : groups) {
            response->add_blockgroups()->Swap(&group);
        }
    }

    response->set_status(err);
}

void SpaceServiceImpl::AcquireBlockGroup(
    google::protobuf::RpcController* /*controller*/,
    const AcquireBlockGroupRequest* request,
    AcquireBlockGroupResponse* response,
    google::protobuf::Closure* done) {
    ClosureGuard doneGuard(done);

    auto* space = spaceMgr_->GetVolumeSpace(request->fsid());
    if (!space) {
        LOG(WARNING) << "Volume space not found, fsid: " << request->fsid()
                     << ", block group offset: " << request->blockgroupoffset();
        response->set_status(SpaceErrNotFound);
        return;
    }

    auto err =
        space->AcquireBlockGroup(request->blockgroupoffset(), request->owner(),
                                 response->mutable_blockgroups());
    if (err != SpaceOk) {
        LOG(WARNING) << "Acquire block group failed, fsId: " << request->fsid()
                     << ", block group offset: " << request->blockgroupoffset()
                     << ", err: " << SpaceErrCode_Name(err);
        response->clear_blockgroups();
    }

    response->set_status(err);
}

void SpaceServiceImpl::ReleaseBlockGroup(
    google::protobuf::RpcController* /*controller*/,
    const ReleaseBlockGroupRequest* request,
    ReleaseBlockGroupResponse* response,
    google::protobuf::Closure* done) {
    ClosureGuard doneGuard(done);

    auto* space = spaceMgr_->GetVolumeSpace(request->fsid());
    if (!space) {
        LOG(WARNING) << "Volume space not found, fsId: " << request->fsid();
        response->set_status(SpaceErrNotFound);
        return;
    }

    std::vector<BlockGroup> groups(request->blockgroups().begin(),
                                   request->blockgroups().end());
    auto err = space->ReleaseBlockGroups(groups);
    if (err != SpaceOk) {
        LOG(WARNING) << "Release block group failed, fsId: " << request->fsid()
                     << ", err: " << SpaceErrCode_Name(err);
    }

    response->set_status(err);
}

void SpaceServiceImpl::StatSpace(
    google::protobuf::RpcController* /*controller*/,
    const StatSpaceRequest* request,
    StatSpaceResponse* response,
    google::protobuf::Closure* done) {
    ClosureGuard doneGuard(done);

    auto* space = spaceMgr_->GetVolumeSpace(request->fsid());
    if (!space) {
        LOG(WARNING) << "Volume space not found, fsid: " << request->fsid();
        response->set_status(SpaceErrNotFound);
        return;
    }

    // TODO(wuhanqing): implement stat space
    response->set_status(SpaceOk);
}

void SpaceServiceImpl::UpdateUsage(
    google::protobuf::RpcController* /*controller*/,
    const UpdateUsageRequest* request,
    UpdateUsageResponse* response,
    google::protobuf::Closure* done) {
    ClosureGuard doneGuard(done);

    auto* space = spaceMgr_->GetVolumeSpace(request->fsid());
    if (!space) {
        LOG(WARNING) << "Volume space not found, fsid: " << request->fsid();
        response->set_status(SpaceErrNotFound);
        return;
    }

    // TODO(wuhanqing): update usage
    response->set_status(SpaceOk);
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs

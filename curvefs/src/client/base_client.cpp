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

#include "curvefs/src/client/base_client.h"
namespace curvefs {
namespace client {

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
}  // namespace client
}  // namespace curvefs

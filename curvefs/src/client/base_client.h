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
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/src/client/common/extent.h"

namespace curvefs {
namespace client {

using curvefs::space::AllocateSpaceRequest;
using curvefs::space::AllocateSpaceResponse;
using curvefs::space::DeallocateSpaceRequest;
using curvefs::space::DeallocateSpaceResponse;
using curvefs::space::Extent;

using common::ExtentAllocInfo;

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

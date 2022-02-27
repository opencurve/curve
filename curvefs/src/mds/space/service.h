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
 * Date: Friday Feb 25 17:13:33 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_MDS_SPACE_SERVICE_H_
#define CURVEFS_SRC_MDS_SPACE_SERVICE_H_

#include "curvefs/proto/space.pb.h"
#include "curvefs/src/mds/space/manager.h"

namespace curvefs {
namespace mds {
namespace space {

class SpaceServiceImpl : public SpaceService {
 public:
    explicit SpaceServiceImpl(SpaceManager* spaceMgr) : spaceMgr_(spaceMgr) {}

    void AllocateBlockGroup(google::protobuf::RpcController* controller,
                            const AllocateBlockGroupRequest* request,
                            AllocateBlockGroupResponse* response,
                            google::protobuf::Closure* done) override;

    void AcquireBlockGroup(google::protobuf::RpcController* controller,
                           const AcquireBlockGroupRequest* request,
                           AcquireBlockGroupResponse* response,
                           google::protobuf::Closure* done) override;

    void ReleaseBlockGroup(google::protobuf::RpcController* controller,
                           const ReleaseBlockGroupRequest* request,
                           ReleaseBlockGroupResponse* response,
                           google::protobuf::Closure* done) override;

    void StatSpace(google::protobuf::RpcController* controller,
                   const StatSpaceRequest* request,
                   StatSpaceResponse* response,
                   google::protobuf::Closure* done) override;

    void UpdateUsage(google::protobuf::RpcController* controller,
                     const UpdateUsageRequest* request,
                     UpdateUsageResponse* response,
                     google::protobuf::Closure* done) override;

 private:
    SpaceManager* spaceMgr_;
};

}  // namespace space
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SPACE_SERVICE_H_

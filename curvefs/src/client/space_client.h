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
 * Created Date: Thur Jun 17 2021
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_CLIENT_SPACE_CLIENT_H_
#define CURVEFS_SRC_CLIENT_SPACE_CLIENT_H_

#include <list>

#include "curvefs/proto/space.pb.h"
#include "curvefs/src/client/base_client.h"
#include "curvefs/src/client/config.h"
#include "curvefs/src/client/error_code.h"
#include "curvefs/src/client/extent.h"

namespace curvefs {
namespace client {

using curvefs::space::Extent;
using curvefs::space::SpaceStatusCode;

class SpaceClient {
 public:
    SpaceClient() {}
    virtual ~SpaceClient() {}

    virtual CURVEFS_ERROR Init(const SpaceAllocServerOption &spaceopt,
                       SpaceBaseClient *baseclient) = 0;

    virtual CURVEFS_ERROR
    AllocExtents(uint32_t fsId,
                 const std::list<ExtentAllocInfo> &toAllocExtents,
                 curvefs::space::AllocateType type,
                 std::list<Extent> *allocatedExtents) = 0;

    virtual CURVEFS_ERROR
    DeAllocExtents(uint32_t fsId, std::list<Extent> allocatedExtents) = 0;
};

class SpaceAllocServerClientImpl : public SpaceClient {
 public:
    using RPCFunc =
        std::function<CURVEFS_ERROR(brpc::Channel *, brpc::Controller *)>;

    CURVEFS_ERROR Init(const SpaceAllocServerOption &spaceopt,
                       SpaceBaseClient *baseclient) override;

    virtual CURVEFS_ERROR AllocExtents(
        uint32_t fsId, const std::list<ExtentAllocInfo> &toAllocExtents,
        curvefs::space::AllocateType type, std::list<Extent> *allocatedExtents);

    virtual CURVEFS_ERROR DeAllocExtents(uint32_t fsId,
                                         std::list<Extent> allocatedExtents);

    void SpaceStatusCode2CurveFSErr(const SpaceStatusCode &statcode,
                                    CURVEFS_ERROR *errcode);

 protected:
    class SpaceAllocRPCExcutor {
     public:
        SpaceAllocRPCExcutor() : opt_() {}
        ~SpaceAllocRPCExcutor() {}
        void SetOption(const SpaceAllocServerOption &option) { opt_ = option; }

        CURVEFS_ERROR DoRPCTask(RPCFunc task);

     private:
        SpaceAllocServerOption opt_;
    };

 private:
    SpaceBaseClient *basecli_;
    SpaceAllocRPCExcutor excutor_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_SPACE_CLIENT_H_

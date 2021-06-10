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


#ifndef CURVEFS_TEST_CLIENT_MOCK_SPACEALLOC_BASE_CLIENT_H_
#define CURVEFS_TEST_CLIENT_MOCK_SPACEALLOC_BASE_CLIENT_H_

#include <gmock/gmock.h>
#include <list>
#include "curvefs/src/client/base_client.h"

namespace curvefs {
namespace client {

class MockSpaceBaseClient : public SpaceBaseClient {
 public:
    MockSpaceBaseClient() : SpaceBaseClient() {}
    ~MockSpaceBaseClient() = default;

    MOCK_METHOD6(AllocExtents,
                 void(uint32_t fsId, const ExtentAllocInfo &toAllocExtent,
                      curvefs::space::AllocateType type,
                      AllocateSpaceResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel));

    MOCK_METHOD5(DeAllocExtents,
                 void(uint32_t fsId, const std::list<Extent> &allocatedExtents,
                      DeallocateSpaceResponse *response, brpc::Controller *cntl,
                      brpc::Channel *channel));
};


}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_SPACEALLOC_BASE_CLIENT_H_

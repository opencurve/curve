/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_TEST_CLIENT_MOCK_SPACE_CLIENT_H_
#define CURVEFS_TEST_CLIENT_MOCK_SPACE_CLIENT_H_


#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <list>

#include "curvefs/src/client/space_client.h"


namespace curvefs {
namespace client {

class MockSpaceClient : public SpaceClient {
 public:
    MockSpaceClient() {}
    ~MockSpaceClient() {}

    MOCK_METHOD2(Init, CURVEFS_ERROR(const SpaceAllocServerOption &spaceopt,
                       SpaceBaseClient *baseclient));

    MOCK_METHOD4(AllocExtents, CURVEFS_ERROR(uint32_t fsId,
        const std::list<ExtentAllocInfo> &toAllocExtents,
        AllocateType type,
        std::list<Extent> *allocatedExtents));

    MOCK_METHOD2(DeAllocExtents, CURVEFS_ERROR(uint32_t fsId,
        std::list<Extent> allocatedExtents));
};


}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_SPACE_CLIENT_H_

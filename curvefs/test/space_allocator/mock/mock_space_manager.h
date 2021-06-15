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

#ifndef CURVEFS_TEST_SPACE_ALLOCATOR_MOCK_MOCK_SPACE_MANAGER_H_
#define CURVEFS_TEST_SPACE_ALLOCATOR_MOCK_MOCK_SPACE_MANAGER_H_

#include <gmock/gmock.h>

#include <vector>

#include "curvefs/src/space_allocator/space_manager.h"

namespace curvefs {
namespace space {

class MockSpaceManager : public SpaceManager {
 public:
    MOCK_METHOD1(InitSpace, SpaceStatusCode(const mds::FsInfo&));
    MOCK_METHOD1(UnInitSpace, SpaceStatusCode(uint32_t));
    MOCK_METHOD4(StatSpace,
                 SpaceStatusCode(uint32_t, uint64_t*, uint64_t*, uint64_t*));
    MOCK_METHOD4(AllocateSpace, SpaceStatusCode(uint32_t fsId, uint32_t size,
                                                const SpaceAllocateHint&,
                                                std::vector<PExtent>*));
    MOCK_METHOD2(DeallocateSpace,
                 SpaceStatusCode(uint32_t,
                                 const ::google::protobuf::RepeatedPtrField<
                                     ::curvefs::space::Extent>&));
    MOCK_METHOD2(AllocateS3Chunk, SpaceStatusCode(uint32_t, uint64_t*));
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_TEST_SPACE_ALLOCATOR_MOCK_MOCK_SPACE_MANAGER_H_

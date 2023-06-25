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
 * @Project: curve
 * @Date: 2021-09-06
 * @Author: chengyi
 */
#ifndef CURVEFS_TEST_MDS_MOCK_CHUNKID_ALLOCATOR_H_
#define CURVEFS_TEST_MDS_MOCK_CHUNKID_ALLOCATOR_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "curvefs/src/mds/chunkid_allocator.h"

namespace curvefs {
namespace mds {

class MockChunkIdAllocatorImpl : public ChunkIdAllocatorImpl {
 public:
    MOCK_METHOD1(GenChunkId, int(uint64_t*));
    MOCK_METHOD3(Init, void(std::shared_ptr<StorageClient> client,
                            std::string chunkIdStoreKey, uint64_t bundleSize));
    MOCK_METHOD1(AllocateBundleIds, int(int));
};
}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_TEST_MDS_MOCK_CHUNKID_ALLOCATOR_H_

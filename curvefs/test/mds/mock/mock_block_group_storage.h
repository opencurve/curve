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
 * Date: Tuesday Mar 01 19:47:08 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_MDS_MOCK_MOCK_BLOCK_GROUP_STORAGE_H_
#define CURVEFS_TEST_MDS_MOCK_MOCK_BLOCK_GROUP_STORAGE_H_

#include <gmock/gmock.h>

#include <vector>

#include "curvefs/src/mds/space/block_group_storage.h"

namespace curvefs {
namespace mds {
namespace space {

class MockBlockGroupStorage : public BlockGroupStorage {
 public:
    MOCK_METHOD3(PutBlockGroup,
                 SpaceErrCode(uint32_t, uint64_t, const BlockGroup&));
    MOCK_METHOD2(RemoveBlockGroup, SpaceErrCode(uint32_t, uint64_t));
    MOCK_METHOD2(ListBlockGroups,
                 SpaceErrCode(uint32_t, std::vector<BlockGroup>*));
};

}  // namespace space
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_MDS_MOCK_MOCK_BLOCK_GROUP_STORAGE_H_

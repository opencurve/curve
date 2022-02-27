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
 * Date: Wednesday Mar 23 16:21:11 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_MDS_MOCK_MOCK_VOLUME_SPACE_H_
#define CURVEFS_TEST_MDS_MOCK_MOCK_VOLUME_SPACE_H_

#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "curvefs/src/mds/space/volume_space.h"

namespace curvefs {
namespace mds {
namespace space {

class MockVolumeSpace : public AbstractVolumeSpace {
 public:
    MOCK_METHOD3(AllocateBlockGroups,
                 SpaceErrCode(uint32_t,
                              const std::string&,
                              std::vector<BlockGroup>*));

    MOCK_METHOD3(AcquireBlockGroup,
                 SpaceErrCode(uint64_t blockGroupOffset,
                              const std::string& owner,
                              BlockGroup* group));

    MOCK_METHOD1(ReleaseBlockGroups,
                 SpaceErrCode(const std::vector<BlockGroup>& blockGroups));
};

}  // namespace space
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_MDS_MOCK_MOCK_VOLUME_SPACE_H_

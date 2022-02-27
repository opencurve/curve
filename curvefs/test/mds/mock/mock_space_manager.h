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
 * Date: Tuesday Mar 01 16:24:17 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_MDS_MOCK_MOCK_SPACE_MANAGER_H_
#define CURVEFS_TEST_MDS_MOCK_MOCK_SPACE_MANAGER_H_

#include <gmock/gmock.h>

#include "curvefs/src/mds/space/manager.h"

namespace curvefs {
namespace mds {
namespace space {

class MockSpaceManager : public SpaceManager {
 public:
    MOCK_CONST_METHOD1(GetVolumeSpace, AbstractVolumeSpace*(uint32_t));
    MOCK_METHOD1(AddVolume, SpaceErrCode(const FsInfo&));
    MOCK_METHOD1(RemoveVolume, SpaceErrCode(uint32_t));
    MOCK_METHOD1(DeleteVolume, SpaceErrCode(uint32_t));
};

}  // namespace space
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_MDS_MOCK_MOCK_SPACE_MANAGER_H_

/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Date: Tue Apr 18 16:34:03 CST 2023
 * Author: lixiaocui
 */

#include <gmock/gmock.h>
#include <vector>
#include "curvefs/src/metaserver/space/volume_space_manager.h"

#ifndef CURVEFS_TEST_METASERVER_SPACE_MOCK_VOLUME_SPACE_MANAGER_H_
#define CURVEFS_TEST_METASERVER_SPACE_MOCK_VOLUME_SPACE_MANAGER_H_

namespace curvefs {
namespace metaserver {

class MockVolumeSpaceManager : public VolumeSpaceManager {
 public:
    MOCK_METHOD3(DeallocVolumeSpace,
                 bool(uint32_t, uint64_t, const std::vector<Extent> &));
    MOCK_METHOD1(Destroy, void(uint32_t));
    MOCK_METHOD1(GetBlockGroupSize, uint64_t(uint32_t));
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_SPACE_MOCK_VOLUME_SPACE_MANAGER_H_

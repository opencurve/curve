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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */


#ifndef CURVEFS_TEST_CLIENT_MOCK_EXTENT_MANAGER_H_
#define CURVEFS_TEST_CLIENT_MOCK_EXTENT_MANAGER_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <list>

#include "curvefs/src/client/extent_manager.h"

using ::testing::Return;
using ::testing::_;

namespace curvefs {
namespace client {

class MockExtentManager : public ExtentManager {
 public:
    MockExtentManager() {}
    ~MockExtentManager() {}

    MOCK_METHOD1(Init, CURVEFS_ERROR(const ExtentManagerOption &options));

    MOCK_METHOD4(GetToAllocExtents, CURVEFS_ERROR(
        const VolumeExtentList &extents,
        uint64_t offset,
        uint64_t len,
        std::list<ExtentAllocInfo> *toAllocExtents));

    MOCK_METHOD3(MergeAllocedExtents, CURVEFS_ERROR(
        const std::list<ExtentAllocInfo> &toAllocExtents,
        const std::list<Extent> &allocatedExtents,
        VolumeExtentList *extents));

    MOCK_METHOD3(MarkExtentsWritten, CURVEFS_ERROR(
        uint64_t offset, uint64_t len,
        VolumeExtentList *extents));

    MOCK_METHOD4(DivideExtents, CURVEFS_ERROR(
        const VolumeExtentList &extents,
        uint64_t offset,
        uint64_t len,
        std::list<PExtent> *pExtents));
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_MOCK_EXTENT_MANAGER_H_

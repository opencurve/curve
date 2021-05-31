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
 * @Date: 2021-06-10 10:04:21
 * @Author: chenwei
 */
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "curvefs/src/mds/fs_storage.h"

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;

namespace curvefs {
namespace mds {
class FSStorageTest: public ::testing::Test {
 protected:
    void SetUp() override {
        return;
    }

    void TearDown() override {
        return;
    }
};

TEST_F(FSStorageTest, test1) {
    MemoryFsStorage storage;
    common::Volume volume;
    MdsFsInfo fs1(1, "name1", 0, 100, 10, volume);
    ASSERT_EQ(FSStatusCode::OK, storage.Insert(fs1));
    ASSERT_EQ(FSStatusCode::FS_EXIST, storage.Insert(fs1));
}
}  // namespace mds
}  // namespace curvefs

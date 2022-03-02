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
 * Created Date: Thur Mar 02 2022
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include "curvefs/src/common/s3util.h"

namespace curvefs {
namespace common {
TEST(ValidNameOfInodeTest, test) {
    LOG(INFO) << "inode = 1, name = 1_16777216_2_0_0";
    ASSERT_FALSE(
        curvefs::common::s3util::ValidNameOfInode("1", "1_16777216_2_0_0"));

    LOG(INFO) << "inode = 16777216, name = 1_16777216_2_0_0";
    ASSERT_TRUE(curvefs::common::s3util::ValidNameOfInode("16777216",
                                                          "1_16777216_2_0_0"));

    LOG(INFO) << "inode = 1, name = 1_16777216_2_0_0";
    ASSERT_FALSE(
        curvefs::common::s3util::ValidNameOfInode("1", "1_16777216_2_0_0"));

    LOG(INFO) << "inode = 16777216, name = 1_1_1_16777216_0";
    ASSERT_FALSE(curvefs::common::s3util::ValidNameOfInode("16777216",
                                                           "1_1_1_16777216_0"));

    LOG(INFO) << "inode = 16777216, name = 1_1_1_16777216";
    ASSERT_FALSE(curvefs::common::s3util::ValidNameOfInode("16777216",
                                                           "1_1_1_16777216"));
}

}  // namespace common
}  // namespace curvefs

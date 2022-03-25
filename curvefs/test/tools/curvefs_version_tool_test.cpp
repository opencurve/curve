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
 * @Date: 2021-10-14
 * @Author: chengyi01
 */

#include "curvefs/src/tools/version/curvefs_version_tool.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace curvefs {
namespace tools {
namespace version {

using ::testing::_;
using ::testing::Invoke;
using ::testing::SetArgPointee;

class VersionToolTest : public testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}

 protected:
    VersionTool versionTool_;
};

TEST_F(VersionToolTest, version_test) {
    ASSERT_EQ(versionTool_.Run(), 0);
}

TEST_F(VersionToolTest, print_help) {
    versionTool_.PrintHelp();
}

}  // namespace version
}  // namespace tools
}  // namespace curvefs

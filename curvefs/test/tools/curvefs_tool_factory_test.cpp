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

#include "curvefs/src/tools/curvefs_tool_factory.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <typeinfo>

#include "curvefs/src/tools/curvefs_tool_abstract_creator.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "curvefs/src/tools/version/curvefs_version_tool.h"

namespace curvefs {
namespace tools {

namespace version {
using ::testing::_;
using ::testing::Invoke;
using ::testing::SetArgPointee;

class FactoryTest : public testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}

 protected:
    CurvefsToolFactory factory_;
};

TEST_F(FactoryTest, create_tool_sucess_test) {
    std::shared_ptr<CurvefsTool> tool =
        factory_.GenerateCurvefsTool(kVersionCmd);
    ASSERT_EQ(typeid(*tool), typeid(VersionTool));
}

TEST_F(FactoryTest, create_tool_fail_test) {
    std::shared_ptr<CurvefsTool> tool = factory_.GenerateCurvefsTool("RTTI");
    ASSERT_EQ(tool, nullptr);
}

TEST_F(FactoryTest, creator_test) {
    std::shared_ptr<CurvefsTool> tool =
        CurvefsToolCreator<VersionTool>().Create();
    ASSERT_EQ(typeid(*tool), typeid(VersionTool));
}

}  // namespace version
}  // namespace tools
}  // namespace curvefs

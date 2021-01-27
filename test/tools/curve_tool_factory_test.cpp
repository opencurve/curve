/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * File Created: 2019-09-29
 * Author: charisu
 */

#include <gtest/gtest.h>
#include "src/tools/curve_tool_factory.h"

namespace curve {
namespace tool {

TEST(CurveToolFactoryTest, GetStatusTool) {
    auto curveTool = CurveToolFactory::GenerateCurveTool("status");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("space");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("chunkserver-list");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("server-list");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("logical-pool-list");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("chunkserver-status");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("mds-status");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("etcd-status");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("client-status");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("client-list");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("snapshot-clone-status");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("cluster-status");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("nothing");
    ASSERT_TRUE(curveTool.get() == nullptr);
}

TEST(CurveToolFactoryTest, GetNameSpaceTool) {
    auto curveTool = CurveToolFactory::GenerateCurveTool("get");
    ASSERT_TRUE(dynamic_cast<NameSpaceTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("list");
    ASSERT_TRUE(dynamic_cast<NameSpaceTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("seginfo");
    ASSERT_TRUE(dynamic_cast<NameSpaceTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("clean-recycle");
    ASSERT_TRUE(dynamic_cast<NameSpaceTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("create");
    ASSERT_TRUE(dynamic_cast<NameSpaceTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("delete");
    ASSERT_TRUE(dynamic_cast<NameSpaceTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("chunk-location");
    ASSERT_TRUE(dynamic_cast<NameSpaceTool *>(curveTool.get()) != nullptr);
}

TEST(CurveToolFactoryTest, GetConsistencyCheck) {
    auto curveTool = CurveToolFactory::GenerateCurveTool("check-consistency");
    ASSERT_TRUE(dynamic_cast<ConsistencyCheck *>(curveTool.get()) != nullptr);
}

TEST(CurveToolFactoryTest, GetCurveCli) {
    auto curveTool = CurveToolFactory::GenerateCurveTool("remove-peer");
    ASSERT_TRUE(dynamic_cast<CurveCli *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("reset-peer");
    ASSERT_TRUE(dynamic_cast<CurveCli *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("transfer-leader");
    ASSERT_TRUE(dynamic_cast<CurveCli *>(curveTool.get()) != nullptr);
}

TEST(CurveToolFactoryTest, GetCopysetCheck) {
    auto curveTool = CurveToolFactory::GenerateCurveTool("check-copyset");
    ASSERT_TRUE(dynamic_cast<CopysetCheck *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("check-chunkserver");
    ASSERT_TRUE(dynamic_cast<CopysetCheck *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("check-server");
    ASSERT_TRUE(dynamic_cast<CopysetCheck *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("copysets-status");
    ASSERT_TRUE(dynamic_cast<CopysetCheck *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("check-operator");
    ASSERT_TRUE(dynamic_cast<CopysetCheck *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("list-may-broken-vol");
    ASSERT_TRUE(dynamic_cast<CopysetCheck *>(curveTool.get()) != nullptr);
}

TEST(CurveToolFactoryTest, GetCopysetTool) {
    auto curveTool =
        CurveToolFactory::GenerateCurveTool("set-copyset-availflag");
    ASSERT_TRUE(dynamic_cast<CopysetTool *>(curveTool.get()) != nullptr);
}
}  // namespace tool
}  // namespace curve

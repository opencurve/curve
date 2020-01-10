/*
 * Project: curve
 * File Created: 2019-09-29
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include "src/tools/curve_tool_factory.h"

DEFINE_string(mdsAddr, "127.0.0.1:6666", "mds addr");
DEFINE_string(etcdAddr, "127.0.0.1:2379", "etcd addr");
DEFINE_uint64(rpcTimeout, 3000, "millisecond for rpc timeout");
DEFINE_uint64(rpcRetryTimes, 5, "rpc retry times");

namespace curve {
namespace tool {

TEST(CurveToolFactoryTest, GetStatusTool) {
    auto curveTool = CurveToolFactory::GenerateCurveTool("status");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("space");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("chunkserver-list");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("chunkserver-status");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("mds-status");
    ASSERT_TRUE(dynamic_cast<StatusTool *>(curveTool.get()) != nullptr);
    curveTool = CurveToolFactory::GenerateCurveTool("etcd-status");
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
    curveTool = CurveToolFactory::GenerateCurveTool("check-cluster");
    ASSERT_TRUE(dynamic_cast<CopysetCheck *>(curveTool.get()) != nullptr);
}

TEST(CurveToolFactoryTest, GetSnapshotCheck) {
    auto curveTool = CurveToolFactory::GenerateCurveTool("snapshot-check");
    ASSERT_TRUE(dynamic_cast<SnapshotCheck *>(curveTool.get()) != nullptr);
}
}  // namespace tool
}  // namespace curve

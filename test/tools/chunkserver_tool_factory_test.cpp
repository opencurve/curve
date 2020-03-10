/*
 * Project: curve
 * File Created: 2019-09-29
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include "src/tools/chunkserver_tool_factory.h"

namespace curve {
namespace tool {

TEST(ChunkServerToolFactoryTest, GenerateChunkserveTool) {
    auto tool = ChunkServerToolFactory::GenerateChunkServerTool("chunk-meta");
    ASSERT_TRUE(dynamic_cast<CurveMetaTool *>(tool.get()) != nullptr);
    tool = ChunkServerToolFactory::GenerateChunkServerTool("raft-log-meta");
    ASSERT_TRUE(dynamic_cast<RaftLogTool *>(tool.get()) != nullptr);
    tool = ChunkServerToolFactory::GenerateChunkServerTool("nothing");
    ASSERT_TRUE(tool.get() == nullptr);
}
}  // namespace tool
}  // namespace curve

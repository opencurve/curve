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

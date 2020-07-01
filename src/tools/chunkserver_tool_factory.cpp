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
 * Created Date: 2020-03-06
 * Author: charisu
 */

#include "src/tools/chunkserver_tool_factory.h"

namespace curve {
namespace tool {

std::shared_ptr<CurveTool> ChunkServerToolFactory::GenerateChunkServerTool(
                                    const std::string& command) {
    if (CurveMetaTool::SupportCommand(command)) {
        return GenerateCurveMetaTool();
    } else if (RaftLogTool::SupportCommand(command)) {
        return GenerateRaftLogTool();
    } else {
        return nullptr;
    }
}

std::shared_ptr<CurveMetaTool> ChunkServerToolFactory::GenerateCurveMetaTool() {
    auto localFs = Ext4FileSystemImpl::getInstance();
    return std::make_shared<CurveMetaTool>(localFs);
}

std::shared_ptr<RaftLogTool> ChunkServerToolFactory::GenerateRaftLogTool() {
    auto localFs = Ext4FileSystemImpl::getInstance();
    auto parser = std::make_shared<SegmentParser>(localFs);
    return std::make_shared<RaftLogTool>(parser);
}

}  // namespace tool
}  // namespace curve

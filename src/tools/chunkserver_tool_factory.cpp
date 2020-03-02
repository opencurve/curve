/*
 * Project: curve
 * Created Date: 2020-03-06
 * Author: charisu
 * Copyright (c) 2018 netease
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

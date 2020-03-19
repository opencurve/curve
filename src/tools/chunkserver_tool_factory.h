/*
 * Project: curve
 * Created Date: 2020-03-06
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_CHUNKSERVER_TOOL_FACTORY_H_
#define SRC_TOOLS_CHUNKSERVER_TOOL_FACTORY_H_

#include <memory>
#include <string>

#include "src/tools/curve_meta_tool.h"
#include "src/tools/raft_log_tool.h"
#include "src/fs/ext4_filesystem_impl.h"

namespace curve {
namespace tool {

using curve::fs::Ext4FileSystemImpl;

class ChunkServerToolFactory {
 public:
    /**
     *  @brief 根据输入的command获取CurveTool对象
     *  @param command 要执行的命令的名称
     *  @return CurveTool实例
     */
    static std::shared_ptr<CurveTool> GenerateChunkServerTool(
                                    const std::string& command);
 private:
    /**
     *  @brief 获取CurveMetaTool实例
     */
    static std::shared_ptr<CurveMetaTool> GenerateCurveMetaTool();

    /**
     *  @brief 获取RaftLogMetaTool实例
     */
    static std::shared_ptr<RaftLogTool> GenerateRaftLogTool();
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CHUNKSERVER_TOOL_FACTORY_H_

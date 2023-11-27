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

#ifndef SRC_TOOLS_CHUNKSERVER_TOOL_FACTORY_H_
#define SRC_TOOLS_CHUNKSERVER_TOOL_FACTORY_H_

#include <memory>
#include <string>

#include "src/fs/ext4_filesystem_impl.h"
#include "src/tools/curve_meta_tool.h"
#include "src/tools/raft_log_tool.h"

namespace curve {
namespace tool {

using curve::fs::Ext4FileSystemImpl;

class ChunkServerToolFactory {
 public:
    /**
     * @brief Retrieve the CurveTool object based on the input command
     * @param command: The name of the command to be executed
     * @return CurveTool instance
     */
    static std::shared_ptr<CurveTool> GenerateChunkServerTool(
        const std::string& command);

 private:
    /**
     * @brief Get CurveMetaTool instance
     */
    static std::shared_ptr<CurveMetaTool> GenerateCurveMetaTool();

    /**
     * @brief Get RaftLogMetaTool instance
     */
    static std::shared_ptr<RaftLogTool> GenerateRaftLogTool();
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CHUNKSERVER_TOOL_FACTORY_H_

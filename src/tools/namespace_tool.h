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
 * Created Date: 2019-09-25
 * Author: charisu
 */

#ifndef SRC_TOOLS_NAMESPACE_TOOL_H_
#define SRC_TOOLS_NAMESPACE_TOOL_H_

#include <gflags/gflags.h>
#include <time.h>

#include <vector>
#include <string>
#include <memory>
#include <iostream>
#include <cstdint>
#include <cstring>
#include <utility>

#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "src/common/timeutility.h"
#include "src/common/string_util.h"
#include "src/mds/common/mds_define.h"
#include "src/tools/namespace_tool_core.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"

using curve::mds::FileInfo;
using curve::mds::PageFileSegment;
using curve::mds::StatusCode;

namespace curve {
namespace tool {

class NameSpaceTool : public CurveTool {
 public:
    explicit NameSpaceTool(std::shared_ptr<NameSpaceToolCore> core) :
                              core_(core), inited_(false) {}

    /**
     * @brief printing usage
     * @param command: Query command
     * @return None
     */
    void PrintHelp(const std::string &command) override;

    /**
     * @brief Execute command
     * @param command: The command executed
     * @return returns 0 for success, -1 for failure
     */
    int RunCommand(const std::string &command) override;

    /**
     * @brief returns whether the command is supported
     * @param command: The command executed
     * @return true/false
     */
    static bool SupportCommand(const std::string& command);

 private:
    // Initialize
    int Init();
    // Print fileInfo and the actual space occupied by the file
    int PrintFileInfoAndActualSize(const std::string& fileName);

    // Print fileInfo and the actual space occupied by the file
    int PrintFileInfoAndActualSize(const std::string& fullName,
                                   const FileInfo& fileInfo);

    // Print file information in the directory
    int PrintListDir(const std::string& dirName);

    // Print out the segment information of the file
    int PrintSegmentInfo(const std::string &fileName);

    // Print fileInfo and convert the time into a readable format for output
    void PrintFileInfo(const FileInfo& fileInfo);

    // Print PageFileSegment and type information for the same chunk on the same line
    void PrintSegment(const PageFileSegment& segment);

    // Print the location information of the chunk
    int PrintChunkLocation(const std::string& fileName,
                                     uint64_t offset);

    // Allocation size of printed files
    int GetAndPrintAllocSize(const std::string& fileName);

    // Print the file size of the directory
    int GetAndPrintFileSize(const std::string& fileName);

    // Currently, curve mds does not support file names in the/test/format, so the/at the end needs to be removed
    void TrimEndingSlash(std::string* fileName);

    int PrintPoolsets();

 private:
    // Core logic
    std::shared_ptr<NameSpaceToolCore> core_;
    // Has initialization been successful
    bool inited_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_NAMESPACE_TOOL_H_

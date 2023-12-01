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
 * Created Date: 2020-02-28
 * Author: charisu
 */

#ifndef SRC_TOOLS_CURVE_META_TOOL_H_
#define SRC_TOOLS_CURVE_META_TOOL_H_

#include <gflags/gflags.h>

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "src/chunkserver/datastore/chunkserver_chunkfile.h"
#include "src/common/bitmap.h"
#include "src/fs/local_filesystem.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"

namespace curve {
namespace tool {

using curve::chunkserver::ChunkFileMetaPage;
using curve::chunkserver::CSErrorCode;
using curve::chunkserver::SnapshotMetaPage;
using curve::common::BitRange;
using curve::fs::LocalFileSystem;

std::ostream& operator<<(std::ostream& os, const vector<BitRange>& ranges);
std::ostream& operator<<(std::ostream& os, const ChunkFileMetaPage& metaPage);
std::ostream& operator<<(std::ostream& os, const SnapshotMetaPage& metaPage);

class CurveMetaTool : public CurveTool {
 public:
    explicit CurveMetaTool(std::shared_ptr<LocalFileSystem> localFs)
        : localFS_(localFs) {}

    /**
     * @brief Execute command
     * @param command The command to be executed
     * @return returns 0 for success, -1 for failure
     */
    int RunCommand(const std::string& command) override;

    /**
     * @brief Print help information
     */
    void PrintHelp(const std::string& command) override;

    /**
     * @brief returns whether the command is supported
     * @param command: The command executed
     * @return true/false
     */
    static bool SupportCommand(const std::string& command);

 private:
    /**
     * @brief Print chunk file metadata
     * @param chunkFileName The file name of the chunk file
     * @return successfully returns 0, otherwise returns -1
     */
    int PrintChunkMeta(const std::string& chunkFileName);

    /**
     * @brief Print snapshot file metadata
     * @param snapFileName The file name of the snapshot file
     * @return successfully returns 0, otherwise returns -1
     */
    int PrintSnapshotMeta(const std::string& snapFileName);

    std::shared_ptr<LocalFileSystem> localFS_;
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CURVE_META_TOOL_H_

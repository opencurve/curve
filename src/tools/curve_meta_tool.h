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
#include "src/common/bitmap.h"
#include "src/fs/local_filesystem.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"
#include "src/chunkserver/datastore/chunkserver_chunkfile.h"

namespace curve {
namespace tool {

using curve::common::BitRange;
using curve::fs::LocalFileSystem;
using curve::chunkserver::ChunkFileMetaPage;
using curve::chunkserver::SnapshotMetaPage;
using curve::chunkserver::CSErrorCode;

std::ostream& operator<<(std::ostream& os, const vector<BitRange>& ranges);
std::ostream& operator<<(std::ostream& os, const ChunkFileMetaPage& metaPage);
std::ostream& operator<<(std::ostream& os, const SnapshotMetaPage& metaPage);

class CurveMetaTool : public CurveTool {
 public:
    explicit CurveMetaTool(std::shared_ptr<LocalFileSystem> localFs) :
                              localFS_(localFs) {}

    /**
     *  @brief 执行命令
     *  @param command 要执行的命令
     *  @return 成功返回0，失败返回-1
    */
    int RunCommand(const std::string& command) override;

    /**
     *  @brief 打印帮助信息
    */
    void PrintHelp(const std::string& command) override;

    /**
     *  @brief 返回是否支持该命令
     *  @param command：执行的命令
     *  @return true / false
     */
    static bool SupportCommand(const std::string& command);

 private:
    /**
     *  @brief 打印chunk文件元数据
     *  @param chunkFileName chunk文件的文件名
     *  @return 成功返回0，否则返回-1
     */
    int PrintChunkMeta(const std::string& chunkFileName);

    /**
     *  @brief 打印快照文件元数据
     *  @param snapFileName 快照文件的文件名
     *  @return 成功返回0，否则返回-1
     */
    int PrintSnapshotMeta(const std::string& snapFileName);

    std::shared_ptr<LocalFileSystem> localFS_;
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CURVE_META_TOOL_H_

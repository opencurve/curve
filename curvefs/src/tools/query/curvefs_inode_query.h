/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-03-25
 * Author: chengyi01
 */

#ifndef CURVEFS_SRC_TOOLS_QUERY_CURVEFS_INODE_QUERY_H_
#define CURVEFS_SRC_TOOLS_QUERY_CURVEFS_INODE_QUERY_H_

#include <brpc/channel.h>
#include <gflags/gflags.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "curvefs/src/tools/list/curvefs_partition_list.h"
#include "curvefs/src/tools/query/curvefs_inode_s3infomap.h"
#include "curvefs/src/tools/query/curvefs_inode.h"

namespace curvefs {
namespace tools {
namespace query {

using InodeBase = curvefs::metaserver::GetInodeRequest;

using PartitionInfoList =
    google::protobuf::RepeatedPtrField<common::PartitionInfo>;

class InodeQueryTool : public CurvefsTool {
 public:
    explicit InodeQueryTool(const std::string& cmd = kInodeQueryCmd,
                            bool show = true)
        : CurvefsTool(cmd),
          partitionListTool_(kNoInvokeCmd, false),
          inodeS3InfoMapTool_(kNoInvokeCmd, false),
          inodeTool_(kNoInvokeCmd, false) {
        show_ = show;
    }
    void PrintHelp() override;
    int Init() override;
    int RunCommand() override;

 protected:
    /**
     * @brief Find in fsId2PartitionList_ with fsid and inodeid and populate
     * remaining fields
     *
     * @param inode
     * @return true
         found
     * @return false not found  
     */
    bool GetInodeInfo(InodeBase* inode);

    std::vector<InodeBase> inodeBases_;
    list::PartitionListTool partitionListTool_;
    InodeS3InfoMapTool inodeS3InfoMapTool_;
    InodeTool inodeTool_;
    std::unordered_map<uint32_t, PartitionInfoList> fsId2PartitionList_;
    std::unordered_map<InodeBase, ChunkInfoList, HashInodeBase,
                       KeyEuqalInodeBase>
        inode2ChunkInfoList_;
    std::unordered_map<InodeBase, std::vector<InodeBaseInfo>, HashInodeBase,
                       KeyEuqalInodeBase>
        inode2InodeBaseInfoList_;
};

}  // namespace query
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_QUERY_CURVEFS_INODE_QUERY_H_

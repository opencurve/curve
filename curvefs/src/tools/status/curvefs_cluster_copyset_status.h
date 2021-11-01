/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-10-31
 * Author: chengyi01
 */
#ifndef CURVEFS_SRC_TOOLS_STATUS_CURVEFS_CLUSTER_COPYSET_STATUS_H_
#define CURVEFS_SRC_TOOLS_STATUS_CURVEFS_CLUSTER_COPYSET_STATUS_H_

#include <memory>
#include <queue>
#include <set>
#include <utility>

#include "curvefs/proto/copyset.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/query/curvefs_copyset_query.h"
#include "curvefs/src/tools/query/curvefs_fsinfo_list.h"
#include "curvefs/src/tools/query/curvefs_partition_list.h"
#include "curvefs/src/tools/status/curvefs_copyset_status.h"

namespace curvefs {
namespace tools {
namespace status {

class ClusterCopysetStatusTool : public CurvefsTool {
 public:
    explicit ClusterCopysetStatusTool(bool show = true)
        : CurvefsTool(kCopysetsStatusCmd) {
        show_ = show;
    }
    void PrintHelp() override;

    int RunCommand() override;
    int Init() override;

 protected:
    std::shared_ptr<query::FsInfoListTool> getFsId_;
    std::shared_ptr<query::PartitionListTool> getPartition_;
    std::shared_ptr<query::CopysetQueryTool> getCopysetinfo_;
    std::shared_ptr<status::CopysetStatusTool> getCopysetStatus_;
};

}  // namespace status
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_STATUS_CURVEFS_CLUSTER_COPYSET_STATUS_H_

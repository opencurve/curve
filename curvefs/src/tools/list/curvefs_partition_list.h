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
 * Created Date: 2022-04-28
 * Author: chengyi01
 */

#ifndef CURVEFS_SRC_TOOLS_LIST_CURVEFS_PARTITION_LIST_H_
#define CURVEFS_SRC_TOOLS_LIST_CURVEFS_PARTITION_LIST_H_

#include <brpc/channel.h>
#include <gflags/gflags.h>

#include <string>
#include <unordered_map>

#include "curvefs/proto/topology.pb.h"
#include "curvefs/proto/common.pb.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"

namespace curvefs {
namespace tools {
namespace list {

using PartitionInfoList =
    google::protobuf::RepeatedPtrField<common::PartitionInfo>;

class PartitionListTool
    : public CurvefsToolRpc<curvefs::mds::topology::ListPartitionRequest,
                            curvefs::mds::topology::ListPartitionResponse,
                            curvefs::mds::topology::TopologyService_Stub> {
 public:
    explicit PartitionListTool(const std::string& cmd = kPartitionListCmd,
                               bool show = true)
        : CurvefsToolRpc(cmd) {
        show_ = show;
    }
    void PrintHelp() override;
    int Init() override;

    std::unordered_map<uint32_t, PartitionInfoList>
    GetFsId2PartitionInfoList() {
        return fsId2PartitionList_;
    }

 protected:
    void AddUpdateFlags() override;
    bool AfterSendRequestToHost(const std::string& host) override;

 protected:
    std::unordered_map<uint32_t, PartitionInfoList> fsId2PartitionList_;
};
}  // namespace list
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_LIST_CURVEFS_PARTITION_LIST_H_

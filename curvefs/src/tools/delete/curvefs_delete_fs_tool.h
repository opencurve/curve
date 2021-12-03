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
 * Created Date: 2021-10-16
 * Author: chengyi01
 */
#ifndef CURVEFS_SRC_TOOLS_DELETE_CURVEFS_DELETE_FS_TOOL_H_
#define CURVEFS_SRC_TOOLS_DELETE_CURVEFS_DELETE_FS_TOOL_H_

#include <brpc/channel.h>
#include <brpc/server.h>
#include <gflags/gflags.h>

#include <string>
#include <iostream>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace tools {
namespace delete_ {

class DeleteFsTool : public CurvefsToolRpc<curvefs::mds::DeleteFsRequest,
                                           curvefs::mds::DeleteFsResponse,
                                           curvefs::mds::MdsService_Stub> {
 public:
    explicit DeleteFsTool(const std::string& cmd = kDeleteFsCmd)
        : CurvefsToolRpc(cmd) {}

    void PrintHelp() override;
    int Init() override;
    void InitHostsAddr() override;
    int RunCommand() override;

 protected:
    void AddUpdateFlags() override;
    bool AfterSendRequestToHost(const std::string& host) override;

 protected:
    uint32_t checkTimes_ = 3;
};

}  // namespace delete_
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_DELETE_CURVEFS_DELETE_FS_TOOL_H_

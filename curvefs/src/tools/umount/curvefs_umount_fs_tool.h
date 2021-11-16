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
 * Created Date: 2021-09-27
 * Author: chengyi01
 */
#ifndef CURVEFS_SRC_TOOLS_UMOUNT_CURVEFS_UMOUNT_FS_TOOL_H_
#define CURVEFS_SRC_TOOLS_UMOUNT_CURVEFS_UMOUNT_FS_TOOL_H_

#include <brpc/channel.h>
#include <brpc/server.h>
#include <gflags/gflags.h>

#include <cstdlib>  // std::system
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>  //NOLINT
#include <utility>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace tools {
namespace umount {

class UmountFsTool : public CurvefsToolRpc<curvefs::mds::UmountFsRequest,
                                           curvefs::mds::UmountFsResponse,
                                           curvefs::mds::MdsService_Stub> {
 public:
    explicit UmountFsTool(const std::string& cmd = kUmountFsCmd)
        : CurvefsToolRpc(cmd) {}
    void PrintHelp() override;

    int RunCommand() override;
    int Init() override;

    void InitHostsAddr() override;

 protected:
    void AddUpdateFlags() override;
    bool AfterSendRequestToHost(const std::string& host) override;
};

}  // namespace umount
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_UMOUNT_CURVEFS_UMOUNT_FS_TOOL_H_

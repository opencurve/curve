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
 * Created Date: 2021-12-23
 * Author: chengyi01
 */

#ifndef CURVEFS_SRC_TOOLS_CREATE_CURVEFS_CREATE_FS_H_
#define CURVEFS_SRC_TOOLS_CREATE_CURVEFS_CREATE_FS_H_

#include <string>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"

namespace curvefs {
namespace tools {
namespace create {

class CreateFsTool : public CurvefsToolRpc<curvefs::mds::CreateFsRequest,
                                           curvefs::mds::CreateFsResponse,
                                           curvefs::mds::MdsService_Stub> {
 public:
    explicit CreateFsTool(const std::string& cmd = kCreateFsCmd,
                          bool show = true)
        : CurvefsToolRpc(cmd) {
        show_ = show;
    }
    void PrintHelp() override;

    int Init() override;

    bool CheckRequiredFlagDefault() override;

 protected:
    void AddUpdateFlags() override;
    bool AfterSendRequestToHost(const std::string& host) override;
};

}  // namespace create
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CREATE_CURVEFS_CREATE_FS_H_

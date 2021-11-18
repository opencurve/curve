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
 * Created Date: 2021-11-18
 * Author: chengyi01
 */
#ifndef CURVEFS_SRC_TOOLS_QUERY_CURVEFS_FS_QUERY_H_
#define CURVEFS_SRC_TOOLS_QUERY_CURVEFS_FS_QUERY_H_

#include <brpc/channel.h>
#include <gflags/gflags.h>

#include <string>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace tools {
namespace query {

class FsQueryTool : public CurvefsToolRpc<curvefs::mds::GetFsInfoRequest,
                                          curvefs::mds::GetFsInfoResponse,
                                          curvefs::mds::MdsService_Stub> {
 public:
    explicit FsQueryTool(const std::string& cmd = kFsQueryCmd, bool show = true)
        : CurvefsToolRpc(cmd) {
        show_ = show;
    }

    void PrintHelp() override;
    int Init() override;

 protected:
    void AddUpdateFlags() override;
    bool AfterSendRequestToHost(const std::string& host) override;
    std::vector<std::string> requestValueVec_;
};

}  // namespace query
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_QUERY_CURVEFS_FS_QUERY_H_

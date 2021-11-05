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

#ifndef CURVEFS_SRC_TOOLS_STATUS_CURVEFS_COPYSET_STATUS_H_
#define CURVEFS_SRC_TOOLS_STATUS_CURVEFS_COPYSET_STATUS_H_

#include <brpc/channel.h>

#include <memory>
#include <string>

#include "curvefs/proto/copyset.pb.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace tools {
namespace status {

class CopysetStatusTool
    : public CurvefsToolRpc<
          curvefs::metaserver::copyset::CopysetsStatusRequest,
          curvefs::metaserver::copyset::CopysetsStatusResponse,
          curvefs::metaserver::copyset::CopysetService_Stub> {
 public:
    explicit CopysetStatusTool(const std::string& cmd = kNoInvokeCmd,
                               bool show = false)
        : CurvefsToolRpc(cmd) {
        show_ = show;
    }

    int RunCommand();
    int Init() override;

 protected:
    void AddUpdateFlags() override;
    bool AfterSendRequestToHost(const std::string& host) override;
};

}  // namespace status
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_STATUS_CURVEFS_COPYSET_STATUS_H_

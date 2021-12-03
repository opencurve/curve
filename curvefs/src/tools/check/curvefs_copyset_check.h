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

#ifndef CURVEFS_SRC_TOOLS_CHECK_CURVEFS_COPYSET_CHECK_H_
#define CURVEFS_SRC_TOOLS_CHECK_CURVEFS_COPYSET_CHECK_H_

#include <brpc/channel.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "curvefs/proto/copyset.pb.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace tools {
namespace check {

using CopysetStatusType = curvefs::metaserver::copyset::CopysetStatus;

class CopysetCheckTool
    : public CurvefsToolRpc<
          curvefs::metaserver::copyset::CopysetsStatusRequest,
          curvefs::metaserver::copyset::CopysetsStatusResponse,
          curvefs::metaserver::copyset::CopysetService_Stub> {
 public:
    explicit CopysetCheckTool(const std::string& cmd = kCopysetCheckCmd,
                              bool show = false)
        : CurvefsToolRpc(cmd) {
        show_ = show;
    }
    void PrintHelp() override;
    int RunCommand();
    int Init() override;
    std::map<uint64_t, std::vector<CopysetStatusType>> GetKey2CopysetStatus() {
        return key2CopysetStatus_;
    }

 protected:
    void AddUpdateFlags() override;
    bool AfterSendRequestToHost(const std::string& host) override;

 protected:
    std::vector<uint64_t> key_;
    std::map<uint64_t, std::vector<CopysetStatusType>> key2CopysetStatus_;
};

}  // namespace check
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CHECK_CURVEFS_COPYSET_CHECK_H_

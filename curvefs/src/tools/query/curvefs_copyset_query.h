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
 * Created Date: 2021-10-30
 * Author: chengyi01
 */

#ifndef CURVEFS_SRC_TOOLS_QUERY_CURVEFS_COPYSET_QUERY_H_
#define CURVEFS_SRC_TOOLS_QUERY_CURVEFS_COPYSET_QUERY_H_

#include <brpc/channel.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/topology/deal_peerid.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"
#include "curvefs/src/tools/status/curvefs_copyset_status.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace tools {
namespace query {

using InfoType = curvefs::mds::topology::CopysetValue;
using StatusType = curvefs::metaserver::copyset::CopysetStatusResponse;

class CopysetQueryTool
    : public CurvefsToolRpc<curvefs::mds::topology::GetCopysetsInfoRequest,
                            curvefs::mds::topology::GetCopysetsInfoResponse,
                            curvefs::mds::topology::TopologyService_Stub> {
 public:
    explicit CopysetQueryTool(const std::string& cmd = kCopysetQueryCmd,
                              bool show = true)
        : CurvefsToolRpc(cmd) {
        show_ = show;
    }
    void PrintHelp() override;
    int Init() override;

 protected:
    void AddUpdateFlags() override;
    bool AfterSendRequestToHost(const std::string& host) override;
    std::map<uint64_t, std::vector<InfoType>> key2Infos_;
    std::map<uint64_t, std::vector<StatusType>> key2Status_;
    std::vector<uint64_t> copysetKeys_;  // key = (poolId << 32) | copysetId
};

}  // namespace query
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_QUERY_CURVEFS_COPYSET_QUERY_H_

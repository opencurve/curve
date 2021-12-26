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
 * Created Date: 2021-11-23
 * Author: chengyi01
 */
#ifndef CURVEFS_SRC_TOOLS_COPYSET_CURVEFS_COPYSET_BASE_TOOL_H_
#define CURVEFS_SRC_TOOLS_COPYSET_CURVEFS_COPYSET_BASE_TOOL_H_

#include <map>
#include <queue>
#include <string>
#include <vector>

#include "curvefs/proto/copyset.pb.h"
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/topology/deal_peerid.h"
#include "curvefs/src/tools/copyset/curvefs_copyset_status.h"

namespace curvefs {
namespace tools {
namespace copyset {

uint64_t GetCopysetKey(uint64_t copysetId, uint64_t poolId);

bool CopysetInfo2CopysetStatus(
    const curvefs::mds::topology::GetCopysetsInfoResponse& response,
    std::map<uint64_t,
             std::vector<curvefs::metaserver::copyset::CopysetStatusResponse>>*
        key2Status);

bool CopysetInfo2CopysetStatus(
    const curvefs::mds::topology::ListCopysetInfoResponse& response,
    std::map<uint64_t,
             std::vector<curvefs::metaserver::copyset::CopysetStatusResponse>>*
        key2Status);

bool Response2CopysetInfo(
    const curvefs::mds::topology::GetCopysetsInfoResponse& response,
    std::map<uint64_t, std::vector<curvefs::mds::topology::CopysetValue>>*
        key2Info);

bool Response2CopysetInfo(
    const curvefs::mds::topology::ListCopysetInfoResponse& response,
    std::map<uint64_t, std::vector<curvefs::mds::topology::CopysetValue>>*
        key2Info);

enum class CheckResult {
    kHealthy = 0,
    // the number of copysetInfo is greater than 1
    kOverCopyset = -1,
    // the number of copysetInfo is less than 1
    kNoCopyset = -2,
    // copyset topo is not ok
    kTopoNotOk = -3,
    // peer not match
    kPeersNoSufficient = -4
};

CheckResult checkCopysetHelthy(
    const std::vector<curvefs::mds::topology::CopysetValue>& copysetInfoVec,
    const std::vector<curvefs::metaserver::copyset::CopysetStatusResponse>&
        copysetStatusVec);

}  // namespace copyset
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_COPYSET_CURVEFS_COPYSET_BASE_TOOL_H_

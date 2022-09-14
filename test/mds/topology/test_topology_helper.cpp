/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: Wed Jul 01 2020
 * Author: xuchaojie
 */

#include "test/mds/topology/test_topology_helper.h"
#include "src/mds/topology/topology_item.h"

namespace curve {
namespace mds {
namespace topology {

bool JudgePoolsetEqual(const Poolset &lh, const Poolset &rh) {
    if (lh.GetId() == rh.GetId() && lh.GetName() == rh.GetName() &&
        lh.GetType() == rh.GetType() && lh.GetDesc() == rh.GetDesc()) {
        return true;
    }
    return false;
}

bool JudgeLogicalPoolEqual(const LogicalPool &lh, const LogicalPool &rh) {
    if (lh.GetId() == rh.GetId() &&
        lh.GetName() == rh.GetName() &&
        lh.GetPhysicalPoolId() == rh.GetPhysicalPoolId() &&
        lh.GetLogicalPoolType() == rh.GetLogicalPoolType() &&
        lh.GetRedundanceAndPlaceMentPolicyJsonStr() ==
            rh.GetRedundanceAndPlaceMentPolicyJsonStr() &&
        lh.GetUserPolicyJsonStr() == rh.GetUserPolicyJsonStr() &&
        lh.GetScatterWidth() == rh.GetScatterWidth() &&
        lh.GetCreateTime() == rh.GetCreateTime() &&
        lh.GetStatus() == rh.GetStatus() &&
        lh.GetLogicalPoolAvaliableFlag() == rh.GetLogicalPoolAvaliableFlag()) {
        return true;
    }
    return false;
}

bool JudgePhysicalPoolEqual(const PhysicalPool &lh, const PhysicalPool &rh) {
    if (lh.GetId() == rh.GetId() &&
        lh.GetName() == rh.GetName() &&
        lh.GetDesc() == rh.GetDesc() &&
        lh.GetDiskCapacity() == rh.GetDiskCapacity()) {
        return true;
    }
    return false;
}

bool JudgeZoneEqual(const Zone &lh, const Zone &rh) {
    if (lh.GetId() == rh.GetId() &&
        lh.GetName() == rh.GetName() &&
        lh.GetPhysicalPoolId() == rh.GetPhysicalPoolId() &&
        lh.GetDesc() == rh.GetDesc()) {
        return true;
    }
    return false;
}

bool JudgeServerEqual(const Server &lh, const Server &rh) {
    if (lh.GetId() == rh.GetId() &&
        lh.GetHostName() == rh.GetHostName() &&
        lh.GetInternalHostIp() == rh.GetInternalHostIp() &&
        lh.GetInternalPort() == rh.GetInternalPort() &&
        lh.GetExternalHostIp() == rh.GetExternalHostIp() &&
        lh.GetExternalPort() == rh.GetExternalPort() &&
        lh.GetZoneId() == rh.GetZoneId() &&
        lh.GetPhysicalPoolId() == rh.GetPhysicalPoolId() &&
        lh.GetDesc() == rh.GetDesc()) {
        return true;
    }
    return false;
}

bool JudgeChunkServerEqual(const ChunkServer &lh, const ChunkServer &rh) {
    if (lh.GetId() == rh.GetId() &&
        lh.GetToken() == rh.GetToken() &&
        lh.GetDiskType() == rh.GetDiskType() &&
        lh.GetServerId() == rh.GetServerId() &&
        lh.GetHostIp() == rh.GetHostIp() &&
        lh.GetPort() == rh.GetPort() &&
        lh.GetMountPoint() == rh.GetMountPoint() &&
        lh.GetStartUpTime() == rh.GetStartUpTime() &&
        lh.GetStatus() == rh.GetStatus() &&
        lh.GetOnlineState() == rh.GetOnlineState() &&
        lh.GetChunkServerState().GetDiskState() ==
            rh.GetChunkServerState().GetDiskState() &&
        lh.GetChunkServerState().GetDiskCapacity() ==
            rh.GetChunkServerState().GetDiskCapacity() &&
        lh.GetChunkServerState().GetDiskUsed() ==
            rh.GetChunkServerState().GetDiskUsed()) {
        return true;
    }
    return false;
}

bool JudgeCopysetInfoEqual(const CopySetInfo &lh, const CopySetInfo &rh) {
    if (lh.GetLogicalPoolId() == rh.GetLogicalPoolId() &&
        lh.GetId() == rh.GetId() &&
        lh.GetEpoch() == rh.GetEpoch() &&
        lh.GetCopySetMembersStr() == rh.GetCopySetMembersStr()) {
        return true;
    }
    return false;
}

}  // namespace topology
}  // namespace mds
}  // namespace curve

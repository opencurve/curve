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
 * Created Date: 2021-09-05
 * Author: wanghai01
 */

#include "curvefs/test/mds/topology/test_topology_helper.h"

namespace curvefs {
namespace mds {
namespace topology {

bool ComparePool(const Pool &lh, const Pool &rh) {
    return lh.GetId() == rh.GetId() &&
           lh.GetName() == rh.GetName() &&
           lh.GetRedundanceAndPlaceMentPolicyJsonStr() ==
           rh.GetRedundanceAndPlaceMentPolicyJsonStr() &&
           lh.GetCreateTime() == rh.GetCreateTime() &&
           lh.GetPoolAvaliableFlag() == rh.GetPoolAvaliableFlag();
}

bool CompareZone(const Zone &lh, const Zone &rh) {
    return lh.GetId() == rh.GetId() &&
           lh.GetName() == rh.GetName() &&
           lh.GetPoolId() == rh.GetPoolId();
}

bool CompareServer(const Server &lh, const Server &rh) {
    return lh.GetId() == rh.GetId() &&
           lh.GetHostName() == rh.GetHostName() &&
           lh.GetInternalIp() == rh.GetInternalIp() &&
           lh.GetInternalPort() == rh.GetInternalPort() &&
           lh.GetExternalIp() == rh.GetExternalIp() &&
           lh.GetExternalPort() == rh.GetExternalPort() &&
           lh.GetZoneId() == rh.GetZoneId() &&
           lh.GetPoolId() == rh.GetPoolId();
}

bool CompareMetaServer(const MetaServer &lh, const MetaServer &rh) {
    return lh.GetId() == rh.GetId() &&
           lh.GetHostName() == rh.GetHostName() &&
           lh.GetToken() == rh.GetToken() &&
           lh.GetServerId() == rh.GetServerId() &&
           lh.GetInternalIp() == rh.GetInternalIp() &&
           lh.GetInternalPort() == rh.GetInternalPort() &&
           lh.GetExternalIp() == rh.GetExternalIp() &&
           lh.GetExternalPort() == rh.GetExternalPort() &&
           lh.GetStartUpTime() == rh.GetStartUpTime() &&
           lh.GetMetaServerSpace().GetDiskCapacity() ==
               rh.GetMetaServerSpace().GetDiskCapacity() &&
           lh.GetMetaServerSpace().GetDiskUsed() ==
               rh.GetMetaServerSpace().GetDiskUsed() &&
           lh.GetMetaServerSpace().GetMemoryUsed() ==
               rh.GetMetaServerSpace().GetMemoryUsed() &&
           lh.GetDirtyFlag() == rh.GetDirtyFlag();
}

bool CompareCopysetInfo(const CopySetInfo &lh, const CopySetInfo &rh) {
    return lh.GetPoolId() == rh.GetPoolId() &&
           lh.GetId() == rh.GetId() &&
           lh.GetEpoch() == rh.GetEpoch() &&
           lh.GetCopySetMembersStr() == rh.GetCopySetMembersStr();
}

bool ComparePartition(const Partition &lh, const Partition &rh) {
    return lh.GetFsId() == rh.GetFsId() &&
           lh.GetPoolId() == rh.GetPoolId() &&
           lh.GetCopySetId() == rh.GetCopySetId() &&
           lh.GetPartitionId() == rh.GetPartitionId() &&
           lh.GetIdStart() == rh.GetIdStart() &&
           lh.GetIdEnd() == rh.GetIdEnd() &&
           lh.GetTxId() == rh.GetTxId();
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

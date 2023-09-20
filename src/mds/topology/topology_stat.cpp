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
 * Created Date: Thu Jun 20 2019
 * Author: xuchaojie
 */

#include "src/mds/topology/topology_stat.h"

#include <memory>
#include <set>
#include <utility>

#include "src/common/concurrent/concurrent.h"
#include "src/mds/common/mds_define.h"

using ::curve::common::WriteLockGuard;
using ::curve::common::ReadLockGuard;

namespace curve {
namespace mds {
namespace topology {

void TopologyStatImpl::UpdateChunkServerStat(ChunkServerIdType csId,
    const ChunkServerStat &stat) {
    WriteLockGuard wLock(statsLock_);

    PoolIdType belongPhysicalPoolId = UNINTIALIZE_ID;
    int ret = topo_-> GetBelongPhysicalPoolId(csId, &belongPhysicalPoolId);
    if (ret != kTopoErrCodeSuccess) {
        LOG(ERROR) << "UpdateChunkServerStat, GetBelongPhysicalPoolId fail,"
                   << " chunkserverId = "
                   << csId;
        return;
    }
    uint32_t available = chunkFilePoolAllocHelp_->GetAvailable();
    auto it = chunkServerStats_.find(csId);
    if (it != chunkServerStats_.end()) {
        int64_t diff = stat.chunkFilepoolSize - it->second.chunkFilepoolSize;
        physicalPoolStats_[belongPhysicalPoolId].chunkFilePoolSize += diff;
        int64_t diffUsed = stat.chunkSizeUsedBytes -
            it->second.chunkSizeUsedBytes;
        physicalPoolStats_[belongPhysicalPoolId].chunkFilePoolUsed += diffUsed;

        if (chunkFilePoolAllocHelp_->GetUseChunkFilepool()) {
            if (stat.chunkSizeUsedBytes >
                    (stat.chunkFilepoolSize * available / 100)) {
                auto rt = physicalPoolStats_[belongPhysicalPoolId]
                    .almostFullCsList
                    .emplace(csId);
                if (rt.second) {
                    LOG(WARNING) << "Find chunkserver is almost full,"
                        << " chunkserverId = "
                        << csId
                        << ", chunkFilePoolSize = "
                        << stat.chunkFilepoolSize
                        << ", chunkFilePoolUsed = "
                        << stat.chunkSizeUsedBytes
                        << ", diff = "
                        << diff
                        << ", diffUsed = "
                        << diffUsed;
                }
            } else {
                auto ct = physicalPoolStats_[belongPhysicalPoolId]
                    .almostFullCsList
                    .erase(csId);
                if (ct != 0) {
                    LOG(INFO) << "Find chunkserver is not full,"
                        << "remove from almost full list, chunkserverId = "
                        << csId
                        << ", chunkFilePoolSize = "
                        << stat.chunkFilepoolSize
                        << ", chunkFilePoolUsed = "
                        << stat.chunkSizeUsedBytes
                        << ", diff = "
                        << diff
                        << ", diffUsed = "
                        << diffUsed;
                }
            }
        }
        it->second = stat;
    } else {
        chunkServerStats_.emplace(csId, stat);
        physicalPoolStats_[belongPhysicalPoolId].chunkFilePoolSize +=
            stat.chunkFilepoolSize;
        physicalPoolStats_[belongPhysicalPoolId].chunkFilePoolUsed +=
            stat.chunkSizeUsedBytes;
        LOG(INFO) << "UpdateChunkServerStat, chunkserver stat add,"
                  << " chunkserverId = "
                  << csId
                  << ", chunkFilePoolSize = "
                  << stat.chunkFilepoolSize
                  << ", chunkFilePoolUsed = "
                  << stat.chunkSizeUsedBytes;
    }
    return;
}

bool TopologyStatImpl::GetChunkServerStat(ChunkServerIdType csId,
    ChunkServerStat *stat) {
    ReadLockGuard rLock(statsLock_);
    auto it = chunkServerStats_.find(csId);
    if (it != chunkServerStats_.end()) {
        *stat = it->second;
        return true;
    }
    return false;
}

bool TopologyStatImpl::GetPhysicalPoolStat(PoolIdType pId,
    PhysicalPoolStat* stat) {
    ReadLockGuard rLock(statsLock_);
    auto it = physicalPoolStats_.find(pId);
    if (it != physicalPoolStats_.end()) {
        *stat = it->second;
        return true;
    }
    return false;
}

int TopologyStatImpl::Init() {
    return kTopoErrCodeSuccess;
}

}  // namespace topology
}  // namespace mds
}  // namespace curve

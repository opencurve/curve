/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-06-27
 * Author: xuchaojie
 */


#include "src/chunkserver/epoch_map.h"

namespace curve {
namespace chunkserver {

bool EpochMap::UpdateEpoch(uint64_t fileId, uint64_t epoch) {
    {
        common::ReadLockGuard rlg(mapMutex_);
        auto it = internalMap_.find(fileId);
        if (it != internalMap_.end()) {
            uint64_t current = it->second.load(std::memory_order_acquire);
            if (current > epoch) {
                LOG(WARNING) << "UpdateEpoch failed, current: (" << fileId
                             << ", " << current
                             << "), target epoch: " << epoch;
                return false;
            }

            while (!it->second.compare_exchange_strong(
                        current, epoch, std::memory_order_acq_rel)) {
                if (current > epoch) {
                    LOG(WARNING) << "UpdateEpoch failed, current: (" << fileId
                                 << ", " << current
                                 << "), target epoch: " << epoch;
                    return false;
                }
            }
            return true;
        }
    }
    {
        common::WriteLockGuard wlg(mapMutex_);
        auto it = internalMap_.find(fileId);
        if (it != internalMap_.end()) {
            if (it->second.load(std::memory_order_acquire) <= epoch) {
                it->second.store(epoch, std::memory_order_release);
                return true;
            } else {
                LOG(WARNING) << "UpdateEpoch failed, current: ("
                    << fileId << ", "
                    << it->second.load(std::memory_order_acquire)
                    << "), target epoch: " << epoch;
                return false;
            }
        } else {
            internalMap_.emplace(fileId, epoch);
            return true;
        }
    }
}

bool EpochMap::CheckEpoch(uint64_t fileId, uint64_t epoch) {
    common::ReadLockGuard rlg(mapMutex_);
    auto it = internalMap_.find(fileId);
    if (it != internalMap_.end()) {
        if (it->second.load(std::memory_order_acquire) == epoch) {
            return true;
        } else {
            return false;
        }
    } else {
        return true;
    }
}


}  // namespace chunkserver
}  // namespace curve

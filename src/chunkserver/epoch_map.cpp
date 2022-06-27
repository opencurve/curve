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
            common::WriteLockGuard ewlg(it->second->epochMutex_);
            if (it->second->epoch <= epoch) {
                it->second->epoch = epoch;
                return true;
            } else {
                return false;
            }
        }
    }
    {
        common::WriteLockGuard wlg(mapMutex_);
        auto it = internalMap_.find(fileId);
        if (it != internalMap_.end()) {
            auto item = std::make_shared<EpochMap::VolumeEpoch>(epoch);
            internalMap_.emplace(fileId, item);
            return true;
        } else {
            if (it->second->epoch <= epoch) {
                it->second->epoch = epoch;
                return true;
            } else {
                return false;
            }
        }
    }
}

bool EpochMap::CheckEpoch(uint64_t fileId, uint64_t epoch) {
    common::ReadLockGuard rlg(mapMutex_);
    auto it = internalMap_.find(fileId);
    if (it != internalMap_.end()) {
        common::ReadLockGuard erlg(it->second->epochMutex_);
        if (it->second->epoch <= epoch) {
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

/*
 *  Copyright (c) 2023 NetEase Inc.
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

#include "curvefs/src/client/rpcclient/fsquota_checker.h"

#include "curvefs/src/client/rpcclient/fsdelta_updater.h"

namespace curvefs {

namespace client {

void FsQuotaChecker::Init() {
    fsCapacityCache_.store(0);
    fsUsedBytesCache_.store(0);
}

bool FsQuotaChecker::QuotaBytesCheck(uint64_t incBytes) {
    uint64_t capacity = fsCapacityCache_.load();
    uint64_t usedBytes = fsUsedBytesCache_.load();
    // quota disabled
    if (capacity == 0) {
        return true;
    }
    // need consider local delta
    auto localDelta = FsDeltaUpdater::GetInstance().GetDeltaBytes();
    return capacity - usedBytes >= localDelta + incBytes;
}

void FsQuotaChecker::UpdateQuotaCache(uint64_t capacity, uint64_t usedBytes) {
    fsCapacityCache_.store(capacity);
    fsUsedBytesCache_.store(usedBytes);
}

}  // namespace client

}  // namespace curvefs

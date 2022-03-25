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
 * Project: Curve
 * Date: 2022-02-18
 * Author: Jingli Chen (Wine93)
 */

#include <glog/logging.h>

#include <string>
#include <memory>

#include "curvefs/src/metaserver/storage/utils.h"
#include "curvefs/src/metaserver/storage/memory_storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

MemoryStorage::MemoryStorage(StorageOptions options)
    : options_(options) {}

bool MemoryStorage::GetStatistics(StorageStatistics* statistics) {
    statistics->maxMemoryQuotaBytes = options_.maxMemoryQuotaBytes;
    statistics->maxDiskQuotaBytes = options_.maxDiskQuotaBytes;

    // memory usage bytes
    uint64_t vmRSS;
    if (!GetProcMemory(&vmRSS)) {
        return false;
    }

    // vmRSS is KB, change it to Byte
    statistics->memoryUsageBytes = vmRSS * 1024;

    // disk usage bytes
    uint64_t total, available;
    if (!GetFileSystemSpaces(options_.dataDir, &total, &available)) {
        LOG(ERROR) << "Get filesystem space failed.";
        return false;
    }
    statistics->diskUsageBytes = total - available;

    return true;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

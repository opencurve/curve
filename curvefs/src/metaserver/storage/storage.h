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
 * Date: 2022-02-14
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_METASERVER_STORAGE_STORAGE_H_
#define CURVEFS_SRC_METASERVER_STORAGE_STORAGE_H_

#include <string>
#include <memory>

#include "curvefs/src/metaserver/storage/config.h"

namespace curvefs {
namespace metaserver {
namespace storage {

struct StorageStatistics {
    uint64_t maxMemoryQuotaBytes;
    uint64_t maxDiskQuotaBytes;
    uint64_t memoryUsageBytes;
    uint64_t diskUsageBytes;
};

class KVStorage {
 public:
    virtual bool GetStatistics(StorageStatistics* Statistics) = 0;
};

void InitStorage(StorageOptions options);

std::shared_ptr<KVStorage> GetStorageInstance();

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  //  CURVEFS_SRC_METASERVER_STORAGE_STORAGE_H_

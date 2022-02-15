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

//#include "rocksdb/db.h"
#include "curvefs/src/metaserver/storage/config.h"

namespace curvefs {
namespace metaserver {
namespace storage {

struct StorageStatistics {
    uint64_t MaxMemoryBytes;
    uint64_t MaxDiskQuotaBytes;
    uint64_t MemoryUsageBytes;
    uint64_t DiskUsageBytes;
};

// The interface inspired by redis, see also https://redis.io/commands
class KVStorage {
 public:
    enum class STORAGE_TYPE {
        MEMORY_STORAGE,
        ROCKSDB_STORAGE,
    };

 public:
    virtual STORAGE_TYPE Type() = 0;

    virtual bool GetStatistics(StorageStatistics* Statistics) = 0;

    // unordered
    virtual Status HGet(const std::string& name,
                        const std::string& key,
                        std::string* value) = 0;

    virtual Status HSet(const std::string& name,
                        const std::string& key,
                        const std::string& value) = 0;

    virtual Status HDel(const std::string& name, const std::string& key) = 0;

    virtual Iterator HGetAll(const std::string& name) = 0;

    virtual Status HClear(const std::string& name) = 0;

    // ordered
    virtual Status SGet(const std::string& name,
                        const std::string& key,
                        std::string* value) = 0;

    virtual Status SSet(const std::string& name,
                        const std::string& key,
                        const std::string& value) = 0;

    virtual Status SDel(const std::string& name, const std::string& key) = 0;

    virtual Iterator SRange(const std::string& name,
                            const std::string& prefix) = 0;

    virtual Iterator SGetAll(const std::string& name) = 0;

    virtual Status SClear(const std::string& name) = 0;
};

static bool InitStorage(StorageOptions options);

static std::shared_ptr<KVStorage> GetStorageInstance();

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  //  CURVEFS_SRC_METASERVER_STORAGE_STORAGE_H_

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

#include <memory>

#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/storage/memory_storage.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::MemoryStorage;
using ::curvefs::metaserver::storage::RocksDBStorage;

static std::shared_ptr<KVStorage> kStorage;

bool InitStorage(StorageOptions options) {
    std::shared_ptr<KVStorage> kvStorage;
    if (options.Type == "memory") {
        kvStorage = std::make_shared<MemoryStorage>(options);
    } else {
        kvStorage = std::make_shared<RocksDBStorage>(options);
    }
    return kvStorage->Open();
}

std::shared_ptr<KVStorage> GetStorageInstance() {
    return kStorage;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

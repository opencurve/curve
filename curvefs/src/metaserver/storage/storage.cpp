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

#include <memory>

#include "curvefs/src/metaserver/storage/storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curvefs::metaserver::storage::Status;
using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::MemoryStorage;
using ::curvefs::metaserver::storage::RocksDBStorage;

static std::shared_ptr<KVStorage> kStorage;

static bool InitStorage(StorageOptions options) {
    bool succ = true;
    if (options.Type == "memory") {
        kStorage = std::make_shared<MemoryStorage>(options);
    } else {
        auto storage = std::make_shared<RocksDBStorage>(options);
        Status s = storage->Open(options.DataDir)
        succ = s.ok();
    }
    return succ;
}

static std::shared_ptr<KVStorage> GetStorageInstance() {
    return kStorage;
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  //  CURVEFS_SRC_METASERVER_STORAGE_STORAGE_H_

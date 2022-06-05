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

#ifndef CURVEFS_SRC_METASERVER_STORAGE_CONFIG_H_
#define CURVEFS_SRC_METASERVER_STORAGE_CONFIG_H_

#include <string>

namespace curvefs {
namespace metaserver {
namespace storage {

struct StorageOptions {
    StorageOptions() {}

    std::string type;

    uint64_t maxMemoryQuotaBytes;

    uint64_t maxDiskQuotaBytes;

    std::string dataDir;

    // only memory storage interested the below config item
    bool compression;

    // only rocksdb storage interested the below config item
    uint64_t unorderedWriteBufferSize;

    uint64_t unorderedMaxWriteBufferNumber;

    uint64_t orderedWriteBufferSize;

    uint64_t orderedMaxWriteBufferNumber;

    uint64_t blockCacheCapacity;

    double memtablePrefixBloomSizeRatio;

    uint64_t statsDumpPeriodSec;

    size_t keyPrefixLength;

    uint64_t s3MetaLimitSizeInsideInode;
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_STORAGE_CONFIG_H_

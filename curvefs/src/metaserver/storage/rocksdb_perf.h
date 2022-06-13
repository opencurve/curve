
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
 * Date: 2022-06-10
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_METASERVER_STORAGE_ROCKSDB_PERF_H_
#define CURVEFS_SRC_METASERVER_STORAGE_ROCKSDB_PERF_H_

#include <string>
#include <random>

#include "curvefs/src/metaserver/storage/storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

enum OPERATOR_TYPE : unsigned char {
    OP_GET = 1,
    OP_PUT = 2,
    OP_DELETE = 3,
    OP_DELETE_RANGE = 4,
    OP_GET_ITERATOR = 5,
    OP_GET_SNAPSHOT = 6,
    OP_CLEAR_SNAPSHOT = 7,
    OP_ITERATOR_SEEK_TO_FIRST = 8,
    OP_ITERATOR_GET_KEY = 9,
    OP_ITERATOR_GET_VALUE = 10,
    OP_ITERATOR_NEXT = 11,
};

class RocksDBPerf {
 public:
    explicit RocksDBPerf(StorageOptions options);

    void Start(OPERATOR_TYPE opType);

    void Stop();

 private:
  double RandomDouble(uint64_t start, uint64_t end);

 private:
    bool enable_;
    uint32_t slowUs_;
    double samplingRatio_;
    OPERATOR_TYPE currentOpType_;
    uint64_t startTimeUs_;
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  //  CURVEFS_SRC_METASERVER_STORAGE_ROCKSDB_PERF_H_

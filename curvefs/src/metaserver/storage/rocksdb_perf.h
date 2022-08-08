
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

#include "rocksdb/perf_context.h"
#include "rocksdb/iostats_context.h"
#include "curvefs/src/metaserver/storage/storage.h"

/*
 *   0: kDisable                             // disable perf start
 *   1: kEnableCount                         // enable only count stats
 *   2: kEnableTimeAndCPUTimeExceptForMutex  // Other than count stats, also enable time
 *                                           // stats except for mutexes
 *                                           // Other than time, also measure CPU time counters. Still don't measure
 *                                           // time (neither wall time nor CPU time) for mutexes.
 *   3: kEnableTimeExceptForMutex
 *   4: kEnableTime                          // enable count and time stats
 * see also: https://github.com/facebook/rocksdb/wiki/Perf-Context-and-IO-Stats-Context#profile-levels-and-costs
 *
 * You can also modify rocksdb perf level to 1 on the fly:
 * curl -s http://127.0.0.1:9000/flags/rocksdb_perf_level?setvalue=1
 */
DECLARE_uint32(rocksdb_perf_level);
DECLARE_uint64(rocksdb_perf_slow_us);
DECLARE_double(rocksdb_perf_sampling_ratio);

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
    OP_BEGIN_TRANSACTION = 12,
    OP_COMMIT_TRANSACTION = 13,
    OP_ROLLBACK_TRANSACTION = 14,
};

class RocksDBPerfGuard {
 public:
    explicit RocksDBPerfGuard(OPERATOR_TYPE opType);

    ~RocksDBPerfGuard();

 private:
    rocksdb::PerfLevel ToPerfLevel(uint32_t level);

 private:
    OPERATOR_TYPE opType_;
    uint64_t startTimeUs_;
    static const uint32_t kPerfLevelOutOfBounds_;
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  //  CURVEFS_SRC_METASERVER_STORAGE_ROCKSDB_PERF_H_

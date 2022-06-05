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

#include <glog/logging.h>

#include <ostream>
#include <iostream>

#include "butil/fast_rand.h"
#include "src/common/timeutility.h"
#include "curvefs/src/metaserver/storage/rocksdb_perf.h"

static bool pass_uint32(const char*, uint32_t) { return true; }
static bool pass_uint64(const char*, uint64_t) { return true; }
static bool pass_double(const char*, double) { return true; }

DEFINE_uint32(rocksdb_perf_level, 0, "rocksdb perf level");
DEFINE_validator(rocksdb_perf_level, &pass_uint32);
DEFINE_uint64(rocksdb_perf_slow_us, 100,
              "rocksdb perf slow operation microseconds");
DEFINE_validator(rocksdb_perf_slow_us, &pass_uint64);
DEFINE_double(rocksdb_perf_sampling_ratio, 0,
              "rocksdb perf sampling ratio");
DEFINE_validator(rocksdb_perf_sampling_ratio, &pass_double);

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::common::TimeUtility;

const uint32_t RocksDBPerfGuard::kPerfLevelOutOfBounds_ = 5;

std::ostream& operator<<(std::ostream& os, OPERATOR_TYPE type) {
    switch (type) {
        case OP_GET:
            os << "GET";
            break;
        case OP_PUT:
            os << "PUT";
            break;
        case OP_DELETE:
            os << "DELETE";
            break;
        case OP_DELETE_RANGE:
            os << "DELETE_RANGE";
            break;
        case OP_GET_ITERATOR:
            os << "GET_ITERATOR";
            break;
        case OP_GET_SNAPSHOT:
            os << "GET_SNAPSHOT";
            break;
        case OP_CLEAR_SNAPSHOT:
            os << "CLEAR_SNAPSHOT";
            break;
        case OP_ITERATOR_SEEK_TO_FIRST:
            os << "ITERATOR_SEEK_TO_FIRST";
            break;
        case OP_ITERATOR_GET_KEY:
            os << "ITERATOR_GET_KEY";
            break;
        case OP_ITERATOR_GET_VALUE:
            os << "ITERATOR_GET_VALUE";
            break;
        case OP_ITERATOR_NEXT:
            os << "ITERATOR_NEXT";
            break;
        case OP_BEGIN_TRANSACTION:
            os << "BEGIN_TRANSACTION";
            break;
        case OP_COMMIT_TRANSACTION:
            os << "COMMIT_TRANSACTION";
            break;
        case OP_ROLLBACK_TRANSACTION:
            os << "ROLLBACK_TRANSACTION";
            break;
        default:
            os << "UNKNWON";
    }
    return os;
}

rocksdb::PerfLevel RocksDBPerfGuard::ToPerfLevel(uint32_t level) {
    switch (level) {
        case 0:
            return rocksdb::PerfLevel::kDisable;
        case 1:
            return rocksdb::PerfLevel::kEnableCount;
        case 2:
            return rocksdb::PerfLevel::kEnableTimeExceptForMutex;
        case 3:
            return rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex;
        case 4:
            return rocksdb::PerfLevel::kEnableTime;
    }
    return rocksdb::PerfLevel::kDisable;
}

RocksDBPerfGuard::RocksDBPerfGuard(OPERATOR_TYPE opType) {
    rocksdb::PerfLevel level = ToPerfLevel(FLAGS_rocksdb_perf_level);
    if (level == rocksdb::PerfLevel::kDisable) {
        return;
    }

    rocksdb::SetPerfLevel(level);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_iostats_context()->Reset();
    opType_ = opType;
    startTimeUs_ = TimeUtility::GetTimeofDayUs();
}

RocksDBPerfGuard::~RocksDBPerfGuard() {
    rocksdb::PerfLevel level = ToPerfLevel(FLAGS_rocksdb_perf_level);
    if (level == rocksdb::PerfLevel::kDisable) {
        return;
    }

    uint64_t now = TimeUtility::GetTimeofDayUs();
    uint64_t latencyUs = now - startTimeUs_;
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    if (latencyUs <= FLAGS_rocksdb_perf_slow_us &&
        FLAGS_rocksdb_perf_sampling_ratio <= butil::fast_rand_double()) {
        return;
    }

    std::ostringstream oss;
    oss << "latencyUs(" << latencyUs << ")"
        << ", slowUs(" << FLAGS_rocksdb_perf_slow_us << ")"
        << ", perf context(" << rocksdb::get_perf_context()->ToString() << ")"
        << ", iostat context(" << rocksdb::get_iostats_context()->ToString()
        << ")";

    if (latencyUs > FLAGS_rocksdb_perf_slow_us) {
        LOG(WARNING) << "[RockDBPerf] slow operation"
                     << ", opType(" << opType_ << "), " << oss.str();
    } else {
        LOG(INFO) << "[RockDBPerf] sampling operation"
                  << ", opType(" << opType_ << "), " << oss.str();
    }
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs


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

#include "rocksdb/perf_context.h"
#include "rocksdb/iostats_context.h"
#include "src/common/timeutility.h"
#include "curvefs/src/metaserver/storage/rocksdb_perf.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::common::TimeUtility;

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
        default:
            os << "UNKNWON";
    }
    return os;
}

RocksDBPerf::RocksDBPerf(StorageOptions options)
    : enable_(options.perfEnable),
      slowUs_(options.perfSlowOperationUs),
      samplingRatio_(options.perfSamplingRatio) {}

void RocksDBPerf::Start(OPERATOR_TYPE opType) {
    if (!enable_) {
        return;
    }

    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
    rocksdb::get_perf_context()->Reset();
    rocksdb::get_iostats_context()->Reset();
    currentOpType_ = opType;
    startTimeUs_ = TimeUtility::GetTimeofDayUs();
}

void RocksDBPerf::Stop() {
    if (!enable_) {
        return;
    }

    uint64_t now = TimeUtility::GetTimeofDayUs();
    uint64_t latencyUs = now - startTimeUs_;
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
    if (latencyUs <= slowUs_ && samplingRatio_ <= RandomDouble(0, 1)) {
        return;
    }

    std::ostringstream oss;
    oss << "latencyUs(" << latencyUs << "), slowUs(" << slowUs_ << ")"
        << ", perf context(" << rocksdb::get_perf_context()->ToString()
        << "), iostat context("
        << rocksdb::get_iostats_context()->ToString() << ")";

    if (latencyUs > slowUs_) {
        LOG(WARNING) << "[RockDBPerf] slow operation, opType("
                     << currentOpType_ << "), " << oss.str();
    } else {
        LOG(INFO) << "[RockDBPerf] sampling operation, opType("
                  << currentOpType_ << "), " << oss.str();
    }
}

double RocksDBPerf::RandomDouble(uint64_t start, uint64_t end) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_real_distribution<> dis(start, end);
    return dis(gen);
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs


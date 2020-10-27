/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: curve
 * File Created: 20190830
 * Author: lixiaocui
 */

#include <vector>
#include <string>
#include "src/mds/nameserver2/allocstatistic/alloc_statistic_helper.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "proto/nameserver2.pb.h"
#include "src/common/timeutility.h"
#include "src/common/namespace_define.h"

namespace curve {
namespace mds {

using ::curve::common::SEGMENTALLOCSIZEKEYEND;
using ::curve::common::SEGMENTALLOCSIZEKEY;
using ::curve::common::SEGMENTINFOKEYPREFIX;
using ::curve::common::SEGMENTINFOKEYEND;
const int GETBUNDLE = 1000;
int AllocStatisticHelper::GetExistSegmentAllocValues(
    std::map<PoolIdType, int64_t> *out,
    const std::shared_ptr<EtcdClientImp> &client) {
    // Obtain the segmentSize value of corresponding logical pools from Etcd
    std::vector<std::string> allocVec;
    int res = client->List(
        SEGMENTALLOCSIZEKEY, SEGMENTALLOCSIZEKEYEND, &allocVec);
    if (res != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "list [" << SEGMENTALLOCSIZEKEY << ","
                   << SEGMENTALLOCSIZEKEYEND << ") fail, errorCode: "
                   << res;
        return -1;
    }

    // decode the string
    for (auto &item : allocVec) {
        PoolIdType lid;
        uint64_t alloc;
        bool res = NameSpaceStorageCodec::DecodeSegmentAllocValue(
            item, &lid, &alloc);
        if (false == res) {
            LOG(ERROR) << "decode segment alloc value: " << item << " fail";
            continue;
        }
        (*out)[lid] = alloc;
    }
    return 0;
}

int AllocStatisticHelper::CalculateSegmentAlloc(
    int64_t revision, const std::shared_ptr<EtcdClientImp> &client,
    std::map<PoolIdType, int64_t> *out) {
    LOG(INFO) << "start calculate segment alloc, revision: " << revision
              << ", bundle size: " << GETBUNDLE;
    uint64_t startTime = ::curve::common::TimeUtility::GetTimeofDayMs();

    std::string startKey = SEGMENTINFOKEYPREFIX;
    std::vector<std::string> values;
    std::string lastKey;
    do {
        // clear data
        values.clear();
        lastKey.clear();

        // get segments in bundles from Etcd, GETBUNDLE is the number of items
        // to fetch
        int res = client->ListWithLimitAndRevision(
           startKey, SEGMENTINFOKEYEND, GETBUNDLE, revision, &values, &lastKey);
        if (res != EtcdErrCode::EtcdOK) {
            LOG(ERROR) << "list [" << startKey << "," << SEGMENTINFOKEYEND
                       << ") at revision: " << revision
                       << " with bundle: " << GETBUNDLE
                       << " fail, errCode: " << res;
            return -1;
        }

        // decode the obtained value
        int startPos = 1;
        if (startKey == SEGMENTINFOKEYPREFIX) {
            startPos = 0;
        }
        for ( ; startPos < values.size(); startPos++) {
            PageFileSegment segment;
            bool res = NameSpaceStorageCodec::DecodeSegment(
                values[startPos], &segment);
            if (false == res) {
                LOG(ERROR) << "decode segment item{"
                          << values[startPos] << "} fail";
                return -1;
            } else {
                (*out)[segment.logicalpoolid()] += segment.segmentsize();
            }
        }

        startKey = lastKey;
    } while (values.size() >= GETBUNDLE);

    LOG(INFO) << "calculate segment alloc ok, time spend: "
              << (::curve::common::TimeUtility::GetTimeofDayMs() - startTime)
              << " ms";
    return 0;
}
}  // namespace mds
}  // namespace curve

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

#ifndef SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_HELPER_H_
#define SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_HELPER_H_

#include <map>
#include <memory>
#include "src/mds/common/mds_define.h"
#include "src/kvstorageclient/etcd_client.h"

using ::curve::mds::topology::PoolIdType;
using ::curve::kvstorage::EtcdClientImp;

namespace curve {
namespace mds {
extern const int GETBUNDLE;

class AllocStatisticHelper {
 public:
    // 获取记录的physicalPool对应的segment的值
    static int GetExistSegmentAllocValues(
        std::map<PoolIdType, int64_t> *out,
        const std::shared_ptr<EtcdClientImp> &client);

    static int CalculateSegmentAlloc(
        int64_t revision, const std::shared_ptr<EtcdClientImp> &client,
        std::map<PoolIdType, int64_t> *out);
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_HELPER_H_

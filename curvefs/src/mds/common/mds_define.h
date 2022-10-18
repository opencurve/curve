/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-08-24
 * Author: wanghai01
 */

#ifndef CURVEFS_SRC_MDS_COMMON_MDS_DEFINE_H_
#define CURVEFS_SRC_MDS_COMMON_MDS_DEFINE_H_

#include <cstdint>
namespace curvefs {
namespace mds {

namespace topology {

typedef uint32_t FsIdType;
typedef uint32_t PoolIdType;
typedef uint32_t ZoneIdType;
typedef uint32_t ServerIdType;
typedef uint32_t MetaServerIdType;
typedef uint32_t PartitionIdType;
typedef uint32_t CopySetIdType;
typedef uint64_t EpochType;
typedef uint32_t UserIdType;
using MemcacheClusterIdType = uint32_t;

const uint32_t UNINITIALIZE_ID = 0u;
const uint32_t UNINITIALIZE_COUNT = UINT32_MAX;

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_COMMON_MDS_DEFINE_H_

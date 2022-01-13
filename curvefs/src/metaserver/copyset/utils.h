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
 * Date: Thu Aug 12 14:22:26 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_UTILS_H_
#define CURVEFS_SRC_METASERVER_COPYSET_UTILS_H_

#include <cstdint>
#include <string>

#include "curvefs/src/metaserver/common/types.h"
#include "curvefs/src/metaserver/copyset/types.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

inline GroupNid ToGroupNid(PoolId poolId, CopysetId copysetId) {
    return (static_cast<uint64_t>(poolId) << 32) | copysetId;
}

inline GroupId ToGroupId(PoolId poolId, CopysetId copysetId) {
    return std::to_string(ToGroupNid(poolId, copysetId));
}

inline PoolId GetPoolId(GroupNid id) {
    return id >> 32;
}

inline CopysetId GetCopysetId(GroupNid id) {
    return id & ((static_cast<uint64_t>(1) << 32) - 1);
}

// (poolid, copysetid, groupid)
std::string ToGroupIdString(PoolId poolId, CopysetId copysetId);

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_UTILS_H_

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
 * Created Date: Thur Oct 14 2021
 * Author: majie1
 */

#ifndef CURVEFS_SRC_COMMON_S3UTIL_H_
#define CURVEFS_SRC_COMMON_S3UTIL_H_

#include <string>

namespace curvefs {
namespace common {
namespace s3util {

inline std::string GenObjName(uint64_t chunkid, uint64_t index,
                              uint64_t compaction, uint64_t fsid,
                              uint64_t inodeid) {
    return std::to_string(fsid) + "_" + std::to_string(inodeid) + "_" +
           std::to_string(chunkid) + "_" + std::to_string(index) + "_" +
           std::to_string(compaction);
}

}  // namespace s3util
}  // namespace common
}  // namespace curvefs

#endif  // CURVEFS_SRC_COMMON_S3UTIL_H_

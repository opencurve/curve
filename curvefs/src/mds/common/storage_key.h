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
 * Created Date: Thu Jul 22 10:45:43 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_MDS_COMMON_STORAGE_KEY_H_
#define CURVEFS_SRC_MDS_COMMON_STORAGE_KEY_H_

#include <cstdint>

namespace curvefs {
namespace mds {

// All prefix keys here are used to concat with corresponding type of key
// The concated-key is used as the actual key stored in K/V storage(e.g., etcd)
// And all prefix keys must be unique
// NOTE: all prefix keys are start with `fs_`, this's to distingush it from the
//       previous volume prefix key

const char FS_NAME_KEY_PREFIX[] = "fs_01";
const char FS_NAME_KEY_END[] = "fs_02";

const char FS_ID_KEY_PREFIX[] = "fs_02";

const char CHUNKID_NAME_KEY_PREFIX[] = "fs_03";
const char CHUNKID_NAME_KEY_END[] = "fs_04";

constexpr uint32_t COMMON_PREFIX_LENGTH = 5;

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_COMMON_STORAGE_KEY_H_

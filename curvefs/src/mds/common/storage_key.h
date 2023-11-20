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
const char BLOCKGROUP_KEY_PREFIX[] = "fs_04";
const char BLOCKGROUP_KEY_END[] = "fs_05";
const char FS_USAGE_KEY_PREFIX[] = "fs_05";
const char FS_USAGE_KEY_END[] = "fs_06";
const char TS_INFO_KEY_PREFIX[] = "fs_07";

constexpr uint32_t COMMON_PREFIX_LENGTH = 5;

const char POOL_KEY_PREFIX[] = "fs_1001";
const char POOL_KEY_END[] = "fs_1002";
const char ZONE_KEY_PREFIX[] = "fs_1002";
const char ZONE_KEY_END[] = "fs_1003";
const char SERVER_KEY_PREFIX[] = "fs_1003";
const char SERVER_KEY_END[] = "fs_1004";
const char METASERVER_KEY_PREFIX[] = "fs_1004";
const char METASERVER_KEY_END[] = "fs_1005";
const char CLUSTER_KEY[] = "fs_1006";
const char COPYSET_KEY_PREFIX[] = "fs_1007";
const char COPYSET_KEY_END[] = "fs_1008";
const char PARTITION_KEY_PREFIX[] = "fs_1008";
const char PARTITION_KEY_END[] = "fs_1009";
const char MEMCACHE_CLUSTER_KEY_PREFIX[] = "fs_1009";
const char MEMCACHE_CLUSTER_KEY_END[] = "fs_1010";
const char FS_2_MEMCACHE_CLUSTER_KEY_PREFIX[] = "fs_1010";
const char FS_2_MEMCACHE_CLUSTER_KEY_END[] = "fs_1011";

constexpr uint32_t TOPOLOGY_PREFIX_LENGTH = 7;

const char DLOCK_KEY_PREFIX[] = "dlock_01";

constexpr uint32_t DLOCK_PREFIX_LENGTH = 8;

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_COMMON_STORAGE_KEY_H_

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
 * Date: 2022-02-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_METASERVER_STORAGE_UTILS_H_
#define CURVEFS_SRC_METASERVER_STORAGE_UTILS_H_

#include <string>
#include <memory>
#include <unordered_map>

#include "absl/container/btree_set.h"
#include "src/common/concurrent/rw_lock.h"

namespace curvefs {
namespace metaserver {
namespace storage {

using ::curve::common::RWLock;

bool GetFileSystemSpaces(const std::string& path,
                         uint64_t* capacity,
                         uint64_t* available);

bool GetProcMemory(uint64_t* vmRSS);

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  //  CURVEFS_SRC_METASERVER_STORAGE_UTILS_H_

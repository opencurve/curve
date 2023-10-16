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
 * @Project: curve
 * @Date: 2021-06-23 21:05:29
 * @Author: chenwei
 */

#ifndef CURVEFS_SRC_COMMON_DEFINE_H_
#define CURVEFS_SRC_COMMON_DEFINE_H_
#include <cstdint>

#include "curvefs/src/client/filesystem/xattr.h"

namespace curvefs {
const uint64_t ROOTINODEID = 1;
const uint64_t RECYCLEINODEID = 2;
const char RECYCLENAME[] = ".recycle";

using ::curvefs::client::filesystem::XATTR_DIR_FILES;
using ::curvefs::client::filesystem::XATTR_DIR_SUBDIRS;
using ::curvefs::client::filesystem::XATTR_DIR_ENTRIES;
using ::curvefs::client::filesystem::XATTR_DIR_FBYTES;
using ::curvefs::client::filesystem::XATTR_DIR_RFILES;
using ::curvefs::client::filesystem::XATTR_DIR_RSUBDIRS;
using ::curvefs::client::filesystem::XATTR_DIR_RENTRIES;
using ::curvefs::client::filesystem::XATTR_DIR_RFBYTES;
using ::curvefs::client::filesystem::XATTR_DIR_PREFIX;
using ::curvefs::client::filesystem::XATTR_WARMUP_OP;
using ::curvefs::client::filesystem::XATTR_WARMUP_OP_LIST;

}  // namespace curvefs
#endif  // CURVEFS_SRC_COMMON_DEFINE_H_

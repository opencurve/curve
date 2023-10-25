/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-09-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_TEST_CLIENT_VFS_HELPER_HELPER_H_
#define CURVEFS_TEST_CLIENT_VFS_HELPER_HELPER_H_

#define UNIT_TEST 1

#include "curvefs/test/client/vfs/helper/expect.h"
#include "curvefs/test/client/vfs/helper/builder.h"

namespace curvefs {
namespace client {
namespace vfs {

constexpr uint64_t KiB = 1024ULL;
constexpr uint64_t MiB = 1024ULL * KiB;
constexpr uint64_t GiB = 1024ULL * MiB;
constexpr uint64_t TiB = 1024ULL * GiB;

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#undef UNIT_TEST

#endif  // CURVEFS_TEST_CLIENT_VFS_HELPER_HELPER_H_

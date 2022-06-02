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
 * Project: curve
 * Date: Thursday Jun 02 17:31:28 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_COMMON_THREADING_H_
#define CURVEFS_SRC_COMMON_THREADING_H_

namespace curvefs {
namespace common {

// Set current thread's name
void SetThreadName(const char* name);

}  // namespace common
}  // namespace curvefs

#endif  // CURVEFS_SRC_COMMON_THREADING_H_

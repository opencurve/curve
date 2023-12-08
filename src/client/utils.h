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

#ifndef SRC_CLIENT_UTILS_H_
#define SRC_CLIENT_UTILS_H_

#include <cstdint>

namespace curve {
namespace client {

// Adjust the soft limit of open files to hard limit.
// If |limit| equals 0, then directly return true.
// If hard limit is less than |limit| than return false.
bool AdjustOpenFileSoftLimitToHardLimit(uint64_t limit);

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_UTILS_H_

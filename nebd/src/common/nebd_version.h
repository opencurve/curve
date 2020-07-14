/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: nebd
 * Created Date: 2020-03-23
 * Author: charsiu
 */

#ifndef NEBD_SRC_COMMON_NEBD_VERSION_H_
#define NEBD_SRC_COMMON_NEBD_VERSION_H_

#include <string>

namespace nebd {
namespace common {

std::string NebdVersion();
void ExposeNebdVersion();

}  // namespace common
}  // namespace nebd

#endif  // NEBD_SRC_COMMON_NEBD_VERSION_H_

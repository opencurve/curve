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
 * Project: curve
 * Created Date: Thu Aug  5 10:25:59 CST 2021
 * Author: xuchaojie
 */

#ifndef SRC_COMMON_CONCURRENT_NAME_LOCK_H_
#define SRC_COMMON_CONCURRENT_NAME_LOCK_H_

#include "src/common/concurrent/generic_name_lock.h"

namespace curve {
namespace common {

using NameLock = GenericNameLock<Mutex>;
using NameLockGuard = GenericNameLockGuard<Mutex>;

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_CONCURRENT_NAME_LOCK_H_

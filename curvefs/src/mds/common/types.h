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

#ifndef CURVEFS_SRC_MDS_COMMON_TYPES_H_
#define CURVEFS_SRC_MDS_COMMON_TYPES_H_

#include <limits>

#define BTHREAD_MUTEX

#ifdef BTHREAD_MUTEX
#include <bthread/mutex.h>
#else
#include <mutex>
#endif  // BTHREAD_MUTEX

#include "src/common/concurrent/rw_lock.h"

namespace curvefs {
namespace mds {

#ifdef BTHREAD_MUTEX
using Mutex = ::bthread::Mutex;
using RWLock = ::curve::common::BthreadRWLock;
#else
using Mutex = std::mutex;
using RWLock = ::curve::common::RWLock;
#endif  // BTHREAD_MUTEX

using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_COMMON_TYPES_H_

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
 * Date: Fri Aug  6 17:10:54 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_TYPES_H_
#define CURVEFS_SRC_METASERVER_COPYSET_TYPES_H_

#include <braft/configuration.h>
#include <braft/raft.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <cstdint>

#include "src/common/concurrent/rw_lock.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

const char RAFT_DATA_DIR[] = "data";
const char RAFT_META_DIR[] = "raft_meta";
const char RAFT_SNAP_DIR[] = "raft_snapshot";
const char RAFT_LOG_DIR[] = "raft_log";

using GroupId = braft::GroupId;
using GroupNid = uint64_t;

using Mutex = ::bthread::Mutex;
using CondVar = ::bthread::ConditionVariable;
using RWLock = ::curve::common::BthreadRWLock;

using ReadLockGuard = ::curve::common::ReadLockGuard;
using WriteLockGuard = ::curve::common::WriteLockGuard;

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_TYPES_H_

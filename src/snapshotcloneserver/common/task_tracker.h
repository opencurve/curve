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
 * Created Date: Thu Sep 12 2019
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_TASK_TRACKER_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_TASK_TRACKER_H_

#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/common/task_tracker.h"

using ::curve::common::TaskTracker;
using ::curve::common::ContextTaskTracker;

namespace curve {
namespace snapshotcloneserver {

struct RecoverChunkContext;
struct CreateCloneChunkContext;

using RecoverChunkContextPtr = std::shared_ptr<RecoverChunkContext>;
using RecoverChunkTaskTracker = ContextTaskTracker<RecoverChunkContextPtr>;

using CreateCloneChunkContextPtr = std::shared_ptr<CreateCloneChunkContext>;
using CreateCloneChunkTaskTracker =
    ContextTaskTracker<CreateCloneChunkContextPtr>;

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_TASK_TRACKER_H_

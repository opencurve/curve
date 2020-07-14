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
 * Created Date: Wed Mar 18 2020
 * Author: xuchaojie
 */
#include "src/snapshotcloneserver/clone/clone_task.h"

namespace curve {
namespace snapshotcloneserver {

std::ostream& operator<<(std::ostream& os, const CloneTaskInfo &taskInfo) {
    os << "{ CloneInfo : " << taskInfo.GetCloneInfo();
    os << ", Progress : " << taskInfo.GetProgress() << " }";
    return os;
}

}  // namespace snapshotcloneserver
}  // namespace curve

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
 * Created Date: Mon Sep 09 2019
 * Author: xuchaojie
 */

#include "src/snapshotcloneserver/clone/clone_reference.h"


namespace curve {
namespace snapshotcloneserver {

void CloneReference::IncrementRef(const std::string &fileName) {
    curve::common::WriteLockGuard guard(refMapLock_);
    auto it = refMap_.find(fileName);
    if (it != refMap_.end()) {
        it->second++;
    } else {
        refMap_.emplace(fileName, 1);
    }
}

void CloneReference::DecrementRef(const std::string &fileName) {
    curve::common::WriteLockGuard guard(refMapLock_);
    auto it = refMap_.find(fileName);
    if (it != refMap_.end()) {
        it->second--;
        if (0 == it->second) {
            refMap_.erase(it);
        }
    } else {
        LOG(ERROR) << "Error!, DecrementRef cannot find fileName.";
    }
}

int CloneReference::GetRef(const std::string &fileName) {
    curve::common::ReadLockGuard guard(refMapLock_);
    auto it = refMap_.find(fileName);
    if (it != refMap_.end()) {
        return it->second;
    } else {
        return 0;
    }
}




}  // namespace snapshotcloneserver
}  // namespace curve


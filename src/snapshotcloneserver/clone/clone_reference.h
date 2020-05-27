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
 * Created Date: Wed Aug 28 2019
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_REFERENCE_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_REFERENCE_H_

#include <atomic>
#include <map>
#include <string>

#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/concurrent/name_lock.h"

namespace curve {
namespace snapshotcloneserver {

class CloneReference {
 public:
    CloneReference() {}

    curve::common::NameLock& GetLock() {
        return fileLock_;
    }

    void IncrementRef(const std::string &fileName);
    void DecrementRef(const std::string &fileName);

    int GetRef(const std::string &fileName);

 private:
    std::map<std::string, int> refMap_;
    curve::common::RWLock refMapLock_;
    curve::common::NameLock fileLock_;
};




















}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_REFERENCE_H_

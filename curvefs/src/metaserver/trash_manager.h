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
 * Created Date: 2021-08-31
 * Author: xuchaojie
 */

#ifndef CURVEFS_SRC_METASERVER_TRASH_MANAGER_H_
#define CURVEFS_SRC_METASERVER_TRASH_MANAGER_H_

#include <map>
#include <list>
#include <memory>

#include "src/common/string_util.h"

#include "src/common/concurrent/concurrent.h"
#include "curvefs/src/metaserver/trash.h"

namespace curvefs {
namespace metaserver {

class TrashManager {
 public:
    TrashManager() : isStop_(true) {
        LOG(INFO) << "TrashManager";
    }

    static TrashManager& GetInstance() {
        static TrashManager instance_;
        return instance_;
    }

    void Add(uint32_t partitionId, const std::shared_ptr<Trash> &trash);

    void Remove(uint32_t partitionId);

    void Init(const TrashOption &options) {
        options_ = options;
    }

    int Run();

    void Fini();

    void ScanEveryTrash();

    uint64_t Size();

 private:
    void ScanLoop();

 private:
    TrashOption options_;

    Thread recycleThread_;

    Atomic<bool> isStop_;

    InterruptibleSleeper sleeper_;

    std::map<uint32_t, std::shared_ptr<Trash>> trashs_;

    curve::common::RWLock rwLock_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_TRASH_MANAGER_H_

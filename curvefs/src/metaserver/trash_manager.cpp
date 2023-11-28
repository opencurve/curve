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

#include "curvefs/src/metaserver/trash_manager.h"

#include <list>

namespace curvefs {
namespace metaserver {

DECLARE_uint32(trash_scanPeriodSec);

int TrashManager::Run() {
    if (isStop_.exchange(false)) {
        recycleThread_ =
            Thread(&TrashManager::ScanLoop, this);
        LOG(INFO) << "Start trash manager thread ok.";
        return 0;
    }
    return -1;
}

void TrashManager::Fini() {
    if (!isStop_.exchange(true)) {
        LOG(INFO) << "stop trash manager ...";
        sleeper_.interrupt();
        recycleThread_.join();
        options_ = {};
    }
    LOG(INFO) << "stop trash manager ok.";
}

void TrashManager::ScanLoop() {
    while (sleeper_.wait_for(std::chrono::seconds(FLAGS_trash_scanPeriodSec))) {
        ScanEveryTrash();
    }
}

void TrashManager::ScanEveryTrash() {
    VLOG(0) << "whs scan trash start.";
    std::map<uint32_t, std::shared_ptr<Trash>> temp;
    {
        curve::common::ReadLockGuard lg(rwLock_);
        temp = trashs_;
    }
    for (auto &pair : temp) {
        if (!pair.second->IsStop()) {
            pair.second->ScanTrash();
        }
    }
}

void TrashManager::Remove(uint32_t partitionId) {
    curve::common::WriteLockGuard lg(rwLock_);
    auto it = trashs_.find(partitionId);
    if (it != trashs_.end()) {
        LOG(INFO) << "Remove partition from trash manager, partitionId = "
                  << partitionId;
        it->second->StopScan();
        trashs_.erase(it);
    } else {
        LOG(INFO) << "Remove partition from trash manager, "
                  << "partiton not in trash, partitionId = " << partitionId;
    }
}

void TrashManager::ListItems(std::list<TrashItem> *items) {
    items->clear();
    std::map<uint32_t, std::shared_ptr<Trash>> temp;
    {
        curve::common::ReadLockGuard lg(rwLock_);
        temp = trashs_;
    }
    for (auto &pair : temp) {
        std::list<TrashItem> newItems;
        pair.second->ListItems(&newItems);
        items->splice(items->end(), newItems);
    }
}

}  // namespace metaserver
}  // namespace curvefs

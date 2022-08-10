/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * @Project: curve
 * @Date: 2022-08-25 15:38:59
 * @Author: chenwei
 */

#include "curvefs/src/metaserver/recycle_manager.h"

namespace curvefs {
namespace metaserver {
void RecycleManager::Init(const RecycleManagerOption& opt) {
    mdsClient_ = opt.mdsClient;
    metaClient_ = opt.metaClient;
    scanPeriodSec_ = opt.scanPeriodSec;
    scanLimit_ = opt.scanLimit;
}

void RecycleManager::Add(uint32_t partitionId,
                         const std::shared_ptr<RecycleCleaner>& cleaner,
                         copyset::CopysetNode* copysetNode) {
    curve::common::WriteLockGuard lockGuard(rwLock_);
    LOG(INFO) << "Add recycle cleaner to recycle mananager, fsId = "
              << cleaner->GetFsId() << ", partititonId = " << partitionId;
    cleaner->SetCopysetNode(copysetNode);
    cleaner->SetMdsClient(mdsClient_);
    cleaner->SetMetaClient(metaClient_);
    cleaner->SetScanLimit(scanLimit_);
    recycleCleanerList_.push_back(cleaner);
}

void RecycleManager::Remove(uint32_t partitionId) {
    curve::common::WriteLockGuard lockGuard(rwLock_);
    if (inProcessingCleaner_ != nullptr
        && inProcessingCleaner_->GetPartitionId() == partitionId) {
        inProcessingCleaner_->Stop();
        LOG(INFO) << "remove partition from RecycleManager, partition is"
                  << " in processing, stop this cleaner, partitionId = "
                  << partitionId;
        return;
    }

    for (auto it = recycleCleanerList_.begin();
              it != recycleCleanerList_.end(); it++) {
        if ((*it)->GetPartitionId() == partitionId) {
            recycleCleanerList_.erase(it);
            LOG(INFO) << "remove partition from RecycleManager, "
                      << "partitionId = " << partitionId;
            return;
        }
    }
}

void RecycleManager::Run() {
    if (isStop_.exchange(false)) {
        sleeper_.init();
        thread_ = Thread(&RecycleManager::ScanLoop, this);
        LOG(INFO) << "Start RecycleManager thread ok.";
        return;
    }

    LOG(INFO) << "RecycleManager already runned.";
}

void RecycleManager::Stop() {
    if (!isStop_.exchange(true)) {
        LOG(INFO) << "stop RecycleManager manager ...";
        sleeper_.interrupt();
        thread_.join();
        recycleCleanerList_.clear();
        inProcessingCleaner_ = nullptr;
    }
    LOG(INFO) << "stop RecycleManager manager ok.";
}

void RecycleManager::ScanLoop() {
    LOG(INFO) << "RecycleManager start scan thread, scanPeriodSec = "
              << scanPeriodSec_;
    while (sleeper_.wait_for(std::chrono::seconds(scanPeriodSec_))) {
        {
            curve::common::WriteLockGuard lockGuard(rwLock_);
            if (recycleCleanerList_.empty()) {
                continue;
            }

            inProcessingCleaner_ = recycleCleanerList_.front();
            recycleCleanerList_.pop_front();
        }

        uint32_t partitionId = inProcessingCleaner_->GetPartitionId();
        LOG(INFO) << "scan partition, partitionId = " << partitionId
                  << ", fsId = " << inProcessingCleaner_->GetFsId();
        bool deleteRecord = inProcessingCleaner_->ScanRecycle();
        if (deleteRecord) {
            LOG(INFO) << "delete record from clean manager, partitionId = "
                      << partitionId << ", fsId = "
                      << inProcessingCleaner_->GetFsId();
        } else {
            curve::common::WriteLockGuard lockGuard(rwLock_);
            if (!inProcessingCleaner_->IsStop()) {
                recycleCleanerList_.push_back(inProcessingCleaner_);
            } else {
                LOG(INFO) << "scan partition, cleaner is mark stoped, remove it"
                          << ", partitionId = " << partitionId
                          << ", fsId = " << inProcessingCleaner_->GetFsId();
            }
        }
        inProcessingCleaner_ = nullptr;
    }
    LOG(INFO) << "RecycleManager stop scan thread.";
}
}  // namespace metaserver
}  // namespace curvefs

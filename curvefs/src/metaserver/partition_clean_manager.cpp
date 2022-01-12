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
 * @Project: curve
 * @Date: 2021-12-15 10:54:28
 * @Author: chenwei
 */
#include "curvefs/src/metaserver/partition_clean_manager.h"
#include "curvefs/src/metaserver/trash_manager.h"

namespace curvefs {
namespace metaserver {
void PartitionCleanManager::Add(
    uint32_t partitionId, const std::shared_ptr<PartitionCleaner>& cleaner,
    copyset::CopysetNode* copysetNode) {
    curve::common::WriteLockGuard lockGuard(rwLock_);
    LOG(INFO) << "Add partition to partition clean mananager, partititonId = "
              << partitionId;
    cleaner->SetS3Aapter(S3ClientAdaptor_);
    cleaner->SetCopysetNode(copysetNode);
    cleaner->SetIndoDeletePeriod(inodeDeletePeriodMs_);
    partitonCleanerList_.push_back(cleaner);
    partitionCleanerCount << 1;
}

void PartitionCleanManager::Remove(uint32_t partitionId) {
    curve::common::WriteLockGuard lockGuard(rwLock_);
    // 1. first check inProcessingCleaner
    if (inProcessingCleaner_ != nullptr
        && inProcessingCleaner_->GetPartitionId() == partitionId) {
        inProcessingCleaner_->Stop();
        LOG(INFO) << "remove partition from PartitionCleanManager, partition is"
                  << " in processing, stop this cleaner, partitionId = "
                  << partitionId;
        return;
    }

    // 2. then check partitonCleanerList
    for (auto it = partitonCleanerList_.begin();
              it != partitonCleanerList_.end(); it++) {
        if ((*it)->GetPartitionId() == partitionId) {
            partitonCleanerList_.erase(it);
            partitionCleanerCount << -1;
            LOG(INFO) << "remove partition from PartitionCleanManager, "
                      << "partitionId = " << partitionId;
            return;
        }
    }
}

void PartitionCleanManager::Run() {
    if (isStop_.exchange(false)) {
        thread_ = Thread(&PartitionCleanManager::ScanLoop, this);
        LOG(INFO) << "Start PartitionCleanManager thread ok.";
        return;
    }

    LOG(INFO) << "PartitionCleanManager already runned.";
}

void PartitionCleanManager::Fini() {
    if (!isStop_.exchange(true)) {
        LOG(INFO) << "stop PartitionCleanManager manager ...";
        sleeper_.interrupt();
        thread_.join();
        partitonCleanerList_.clear();
        inProcessingCleaner_ = nullptr;
    }
    LOG(INFO) << "stop PartitionCleanManager manager ok.";
}

void PartitionCleanManager::ScanLoop() {
    LOG(INFO) << "PartitionCleanManager start scan thread, scanPeriodSec = "
              << scanPeriodSec_;
    while (sleeper_.wait_for(std::chrono::seconds(scanPeriodSec_))) {
        {
            curve::common::WriteLockGuard lockGuard(rwLock_);
            if (partitonCleanerList_.empty()) {
                continue;
            }

            inProcessingCleaner_ = partitonCleanerList_.front();
            partitonCleanerList_.pop_front();
        }

        uint32_t partitionId = inProcessingCleaner_->GetPartitionId();
        LOG(INFO) << "scan partition, partitionId = " << partitionId;
        bool deleteRecord = inProcessingCleaner_->ScanPartition();
        if (deleteRecord) {
            LOG(INFO) << "scan partition, partition is empty"
                      << ", delete record from clean manager, partitionId = "
                      << partitionId;
            partitionCleanerCount << -1;
        } else {
            curve::common::WriteLockGuard lockGuard(rwLock_);
            if (!inProcessingCleaner_->IsStop()) {
                partitonCleanerList_.push_back(inProcessingCleaner_);
            } else {
                LOG(INFO) << "scan partition, cleaner is mark stoped, remove it"
                          << ", partitionId = " << partitionId;
            }
        }
        inProcessingCleaner_ = nullptr;
    }
    LOG(INFO) << "PartitionCleanManager stop scan thread.";
}

}  // namespace metaserver
}  // namespace curvefs

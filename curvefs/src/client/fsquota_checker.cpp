/*
 *  Copyright (c) 2023 NetEase Inc.
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

#include "curvefs/src/client/fsquota_checker.h"
#include "curvefs/src/client/fsused_updater.h"

namespace curvefs {

namespace client {

void FsQuotaChecker::Init(
    uint32_t fsId, std::shared_ptr<rpcclient::MdsClient> mdsClient,
    std::shared_ptr<rpcclient::MetaServerClient> metaserverClient) {
    fsId_ = fsId;
    fsCapacityCache_.store(0);
    fsUsedBytesCache_.store(0);
    mdsClient_ = mdsClient;
    metaserverClient_ = metaserverClient;
}

bool FsQuotaChecker::QuotaBytesCheck(uint64_t incBytes) {
    uint64_t capacity = fsCapacityCache_.load();
    uint64_t usedBytes = fsUsedBytesCache_.load();
    // quota disabled
    if (capacity == 0) {
        return true;
    }
    // need consider local delta
    auto localDelta = FsUsedUpdater::GetInstance().GetDeltaBytes();
    return capacity - usedBytes >= localDelta + incBytes;
}

void FsQuotaChecker::UpdateQuotaCache() {
    {
        // update quota bytes
        FsInfo fsinfo;
        auto ret = mdsClient_->GetFsInfo(fsId_, &fsinfo);
        if (ret == FSStatusCode::OK) {
            fsCapacityCache_.exchange(fsinfo.capacity());
        }
    }
    {
        // update used
        Inode inode;
        bool streaming;
        auto ret =
            metaserverClient_->GetInode(fsId_, ROOTINODEID, &inode, &streaming);
        if (ret == MetaStatusCode::OK) {
            const auto& xattrs = inode.xattr();
            fsUsedBytesCache_.exchange(
                std::stoull(xattrs.at(curvefs::XATTR_FS_BYTES)));
        }
    }
}

bool UpdateQuotaCacheTask::OnTriggeringTask(timespec* next_abstime) {
    fsQuotaChecker_->UpdateQuotaCache();
    *next_abstime = butil::seconds_from_now(interval_s_);
    return true;
}

void UpdateQuotaCacheTask::OnDestroyingTask() {}

void StartUpdateQuotaCacheTask(FsQuotaChecker* updater, int64_t interval_s) {
    brpc::PeriodicTaskManager::StartTaskAt(
        new UpdateQuotaCacheTask(updater, interval_s),
        butil::seconds_from_now(interval_s));
}

}  // namespace client

}  // namespace curvefs

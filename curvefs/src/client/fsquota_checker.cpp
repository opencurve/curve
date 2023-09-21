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
            if (xattrs.find(curvefs::XATTR_FS_BYTES) != xattrs.end()) {
                // old fs do not have this xattr, be careful
                fsUsedBytesCache_.exchange(
                    std::stoull(xattrs.at(curvefs::XATTR_FS_BYTES)));
            }
        }
    }
}

bool UpdateQuotaCacheTask::OnTriggeringTask(timespec *next_abstime) {
    fsQuotaChecker_->UpdateQuotaCache();
    *next_abstime = butil::seconds_from_now(interval_s_);
    return true;
}

void UpdateQuotaCacheTask::OnDestroyingTask() {}

void StartUpdateQuotaCacheTask(FsQuotaChecker *updater, int64_t interval_s) {
    brpc::PeriodicTaskManager::StartTaskAt(
        new UpdateQuotaCacheTask(updater, interval_s),
        butil::seconds_from_now(interval_s));
}

}  // namespace client


}  // namespace curvefs

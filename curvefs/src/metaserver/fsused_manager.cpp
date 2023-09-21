#include "curvefs/src/metaserver/fsused_manager.h"
#include <unordered_map>

namespace curvefs {
namespace metaserver {

void FsUsedManager::ApplyFsUsedDeltas() {
    std::deque<FsUsedDelta> fsUsedDeltaDirty;
    {
        std::unique_lock<bthread::Mutex> lock_guard(dirtyLock_);
        fsUsedDeltaDirty = std::move(fsUsedDeltas_);
    }
    // merge all deltas
    std::unordered_map<uint32_t, FsUsedDelta> deltaSum;
    for (const auto &d : fsUsedDeltaDirty) {
        auto fsid = d.fsid();
        if (deltaSum.find(fsid) == deltaSum.end()) {
            deltaSum[fsid] = FsUsedDelta();
            deltaSum[fsid].set_fsid(fsid);
            deltaSum[fsid].set_bytes(0);
        }
        if (d.has_bytes()) {
            deltaSum[fsid].set_bytes(deltaSum[fsid].bytes() + d.bytes());
        }
    }
    // send updates
    for (auto &delta : deltaSum) {
        metaserverClient_->UpdateFsUsed(delta.first, delta.second, false);
    }
}

void FsUsedManager::AddFsUsedDelta(FsUsedDelta &&fsUsedDelta) {
    std::unique_lock<bthread::Mutex> lock_guard(dirtyLock_);
    fsUsedDeltas_.emplace_back(std::move(fsUsedDelta));
}

bool UpdateFsUsedTask::OnTriggeringTask(timespec *next_abstime) {
    fsUsedManager_->ApplyFsUsedDeltas();
    *next_abstime = butil::seconds_from_now(interval_s_);
    return true;
}

void UpdateFsUsedTask::OnDestroyingTask() {}

void StartUpdateFsUsedTask(FsUsedManager *fsUsedManager, int64_t interval_s) {
    brpc::PeriodicTaskManager::StartTaskAt(
        new UpdateFsUsedTask(fsUsedManager, interval_s),
        butil::seconds_from_now(interval_s));
}

}  // namespace metaserver
}  // namespace curvefs

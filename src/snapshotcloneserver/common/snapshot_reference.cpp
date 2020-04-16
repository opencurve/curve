/*
 * Project: curve
 * Created Date: Tue Apr 30 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */


#include "src/snapshotcloneserver/common/snapshot_reference.h"

#include <glog/logging.h>

namespace curve {
namespace snapshotcloneserver {


void SnapshotReference::IncrementSnapshotRef(const UUID &snapshotId) {
    curve::common::WriteLockGuard guard(refMapLock_);
    auto it = refMap_.find(snapshotId);
    if (it != refMap_.end()) {
        it->second++;
    } else {
        refMap_.emplace(snapshotId, 1);
    }
}

void SnapshotReference::DecrementSnapshotRef(const UUID &snapshotId) {
    curve::common::WriteLockGuard guard(refMapLock_);
    auto it = refMap_.find(snapshotId);
    if (it != refMap_.end()) {
        it->second--;
        if (0 == it->second) {
            refMap_.erase(it);
        }
    } else {
        LOG(ERROR) << "Error!, DecrementSnapshotRef cannot find snapshotId.";
    }
}

int SnapshotReference::GetSnapshotRef(const UUID &snapshotId) {
    curve::common::ReadLockGuard guard(refMapLock_);
    auto it = refMap_.find(snapshotId);
    if (it != refMap_.end()) {
        return it->second;
    } else {
        return 0;
    }
}



}  // namespace snapshotcloneserver
}  // namespace curve

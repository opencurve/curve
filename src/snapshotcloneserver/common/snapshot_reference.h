/*
 * Project: curve
 * Created Date: Tue Apr 30 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOT_REFERENCE_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOT_REFERENCE_H_

#include <atomic>
#include <map>

#include "src/snapshotcloneserver/common/define.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/concurrent/name_lock.h"

namespace curve {
namespace snapshotcloneserver {

class SnapshotReference {
 public:
    SnapshotReference() {}

    curve::common::NameLock& GetSnapshotLock() {
        return snapshotLock_;
    }

    void IncrementSnapshotRef(const UUID &snapshotId);
    void DecrementSnapshotRef(const UUID &snapshotId);

    int GetSnapshotRef(const UUID &snapshotId);

 private:
    std::map<UUID, curve::common::Atomic<int> > refMap_;
    curve::common::RWLock refMapLock_;

    curve::common::NameLock snapshotLock_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOT_REFERENCE_H_

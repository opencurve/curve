/*
 * Project: curve
 * Created Date: Wed Aug 28 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_REFERENCE_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_REFERENCE_H_

#include <atomic>
#include <map>
#include <string>

#include "src/snapshotcloneserver/common/define.h"
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

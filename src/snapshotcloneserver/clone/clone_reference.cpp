/*
 * Project: curve
 * Created Date: Mon Sep 09 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include "src/snapshotcloneserver/clone/clone_reference.h"


namespace curve {
namespace snapshotcloneserver {

void CloneReference::IncrementRef(const std::string &fileName) {
    curve::common::WriteLockGuard guard(refMapLock_);
    auto it = refMap_.find(fileName);
    if (it != refMap_.end()) {
        it->second++;
    } else {
        refMap_.emplace(fileName, 1);
    }
}

void CloneReference::DecrementRef(const std::string &fileName) {
    curve::common::WriteLockGuard guard(refMapLock_);
    auto it = refMap_.find(fileName);
    if (it != refMap_.end()) {
        it->second--;
        if (0 == it->second) {
            refMap_.erase(it);
        }
    } else {
        LOG(ERROR) << "Error!, DecrementRef cannot find fileName.";
    }
}

int CloneReference::GetRef(const std::string &fileName) {
    curve::common::ReadLockGuard guard(refMapLock_);
    auto it = refMap_.find(fileName);
    if (it != refMap_.end()) {
        return it->second;
    } else {
        return 0;
    }
}




}  // namespace snapshotcloneserver
}  // namespace curve


/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 20190819
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include <set>
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic_helper.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/common/concurrent/concurrent.h"

using ::curve::common::Thread;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

namespace curve {
namespace mds {
int AllocStatistic::Init() {
    // get the current revision
    int res = client_->GetCurrentRevision(&curRevision_);
    if (EtcdErrCode::EtcdOK != res) {
        LOG(ERROR) << "get current revision fail, errCode: " << res;
        return -1;
    }

    res = AllocStatisticHelper::GetExistSegmentAllocValues(
        &existSegmentAllocValues_, client_);
    return res;
}

void AllocStatistic::Run() {
    stop_.store(false);
    periodicPersist_ = Thread(&AllocStatistic::CalculateSegmentAlloc, this);
    calculateAlloc_ = Thread(&AllocStatistic::PeriodicPersist, this);
}

void AllocStatistic::Stop() {
    if (!stop_.exchange(true)) {
        LOG(INFO) << "start stop AllocStatistic...";
        sleeper_.interrupt();
        periodicPersist_.join();
        calculateAlloc_.join();
        LOG(INFO) << "stop AllocStatistic ok!";
    }
}

bool AllocStatistic::GetAllocByLogicalPool(PoolIdType lid, int64_t *alloc) {
    // value segmentAlloc_ is available
    if (true == currentValueAvalible_.load()) {
        ReadLockGuard guard(segmentAllocLock_);
        if (segmentAlloc_.find(lid) != segmentAlloc_.end()) {
            *alloc = segmentAlloc_[lid];
            return true;
        }

        return false;
    // segmentAlloc_ is not available, fetch from existSegmentAllocValues_
    } else {
        ReadLockGuard guard(existSegmentAllocValuesLock_);
        if (existSegmentAllocValues_.find(lid) !=
            existSegmentAllocValues_.end()) {
            *alloc = existSegmentAllocValues_[lid];
            return true;
        }

        return false;
    }

    return false;
}

void AllocStatistic::AllocSpace(
    PoolIdType lid, int64_t changeSize, int64_t revision) {
    // segmentAlloc_ value is not available, changeSize needs to be updated to existSegmentAllocValues_ //NOLINT
    if (false == currentValueAvalible_.load()) {
        WriteLockGuard guarg(existSegmentAllocValuesLock_);
        existSegmentAllocValues_[lid] += changeSize;
    }

    // update change to segmentAlloc_ directly if the Etcd data has been counted
    if (true == segmentAllocFromEtcdOK_.load()) {
        WriteLockGuard guard(segmentAllocLock_);
        segmentAlloc_[lid] += changeSize;
    // if the Etcd data has not been counted, update changeSize to segmentChange_
    } else {
        WriteLockGuard guard(segmentChangeLock_);
        segmentChange_[lid][revision] = changeSize;
    }
}

void AllocStatistic::DeAllocSpace(
    PoolIdType lid, int64_t changeSize, int64_t revision) {
    if (false == currentValueAvalible_.load()) {
        WriteLockGuard guard(existSegmentAllocValuesLock_);
        existSegmentAllocValues_[lid] -= changeSize;
    }

    if (true == segmentAllocFromEtcdOK_.load()) {
        WriteLockGuard guard(segmentAllocLock_);
        segmentAlloc_[lid] -= changeSize;
    } else {
        WriteLockGuard guard(segmentChangeLock_);
        segmentChange_[lid][revision] = 0L - changeSize;
    }
}

void AllocStatistic::CalculateSegmentAlloc() {
    // get the alloc data before revision from Etcd
    int res;
    do {
        res =  AllocStatisticHelper::CalculateSegmentAlloc(
            curRevision_, client_, &segmentAlloc_);
    } while (HandleResult(res));

    LOG(INFO) << "calculate segment alloc revision not bigger than "
              << curRevision_ << " ok";
    // set fetch data from etcd success
    segmentAllocFromEtcdOK_.store(true);

    // sleep for 5s to avoid data remain in segmentChange_ after the merge
    std::this_thread::sleep_for(std::chrono::seconds(5));

    // merge once
    DoMerge();

    // set segmentAlloc_available
    currentValueAvalible_.store(true);
}

bool AllocStatistic::HandleResult(int res) {
    if (res == 0) {
        return false;
    } else {
        // fetch current revision, if failed, retry later
        int errCode = EtcdErrCode::EtcdOK;
        do {
            // if errors occur, sleep for a while and try again
            if (errCode != EtcdErrCode::EtcdOK) {
                LOG(INFO) << "calculateSegmentAlloc"
                          << " occur error, sleep and retry later";
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(retryInterMs_));
            }

            // 1. clear current segmentAlloc_ and updated segmentChange_
            // 2. Regain current revision
            segmentAlloc_.clear();
            errCode = client_->GetCurrentRevision(&curRevision_);
            if (EtcdErrCode::EtcdOK != errCode) {
                LOG(ERROR) << "get current revision error";
            }
        } while (EtcdErrCode::EtcdOK != errCode);
        return true;
    }
}

void AllocStatistic::PeriodicPersist() {
    std::map<PoolIdType, int64_t> lastPersist;
    while (sleeper_.wait_for(
        std::chrono::milliseconds(periodicPersistInterMs_))) {
        std::map<PoolIdType, int64_t> curPersist = GetLatestSegmentAllocInfo();
        if (true == curPersist.empty()) {
            continue;
        }

        // persist the value in curPersist to Etcd
        for (auto &item : curPersist) {
            if (lastPersist.find(item.first) != lastPersist.end()) {
                // if the data is the same as last persistence, do not repeat
                if (lastPersist[item.first] == item.second) {
                    continue;
                } else {
                    lastPersist[item.first] = item.second;
                }
            } else {
                lastPersist[item.first] = item.second;
            }

            int errCode = client_->Put(
                NameSpaceStorageCodec::EncodeSegmentAllocKey(item.first),
                NameSpaceStorageCodec::EncodeSegmentAllocValue(
                    item.first, item.second));

            if (EtcdErrCode::EtcdOK != errCode) {
                LOG(INFO) << "periodic persist logicalPool"
                          << item.first << " size: " << item.second
                          << " to etcd fail, errCode: " << errCode;
                lastPersist.erase(item.first);
            }
        }
        LOG(INFO) << "periodic persist to etcd end";
    }

    lastPersist.clear();
}

void AllocStatistic::DoMerge() {
    // combine the alloc data before and after the revision
    std::set<PoolIdType> logicalPools = GetCurrentLogicalPools();
    for (PoolIdType lid : logicalPools) {
        UpdateSegmentAllocByCurrrevision(lid);
    }
}

std::map<PoolIdType, int64_t> AllocStatistic::GetLatestSegmentAllocInfo() {
    std::map<PoolIdType, int64_t> curPersist;

    // if segmentAlloc_ is available, use it as the current persistent data
    if (true == currentValueAvalible_.load()) {
        ReadLockGuard guard(segmentAllocLock_);
        for (auto item : segmentAlloc_) {
            curPersist[item.first] = item.second;
        }
    // if not, use existSegmentAllocValues_ as this persistent data
    } else {
        ReadLockGuard guard(existSegmentAllocValuesLock_);
        for (auto item : existSegmentAllocValues_) {
            curPersist[item.first] = item.second;
        }
    }

    return curPersist;
}

void AllocStatistic::UpdateSegmentAllocByCurrrevision(PoolIdType lid) {
    // sum up the value after the revision
    int64_t sumChangeUntilNow = 0;

    {
        WriteLockGuard guard(segmentChangeLock_);
        // get the map corresponding to the specified lid
        auto liter = segmentChange_.find(lid);
        if (liter != segmentChange_.end()) {
            // there's no data in the segmentChange_, delete directly
            // The data wil be updated to the segment later
            if (liter->second.empty()) {
                segmentChange_.erase(liter);
            } else {
                // locate the corresponding position of "> curRevision"
                auto removeEnd = liter->second.lower_bound(curRevision_ + 1);
                if (removeEnd != liter->second.end()) {
                    liter->second.erase(liter->second.begin(), removeEnd);
                    for (auto item : liter->second) {
                        sumChangeUntilNow += item.second;
                    }
                    liter->second.clear();
                }
            }
        }
    }


    WriteLockGuard guard(segmentAllocLock_);
    if (segmentAlloc_.find(lid) != segmentAlloc_.end()) {
        segmentAlloc_[lid] += sumChangeUntilNow;
    }
}

std::set<PoolIdType> AllocStatistic::GetCurrentLogicalPools() {
    std::set<PoolIdType> res;
    {
        ReadLockGuard guard(segmentAllocLock_);
        for (auto item : segmentAlloc_) {
            res.insert(item.first);
        }
    }

    {
        ReadLockGuard guard(segmentChangeLock_);
        for (auto item : segmentChange_) {
            res.insert(item.first);
        }
    }

    return res;
}

}  // namespace mds
}  // namespace curve

/*
 * Project: curve
 * Created Date: 20190819
 * Author: lixiaocui
 * Copyright (c) 2018 netease
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
    // 获取当前的revision
    int res = client_->GetCurrentRevision(&curRevision_);
    if (EtcdErrCode::OK != res) {
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
    // segmentAlloc_的值已经可用
    if (true == currentValueAvalible_.load()) {
        ReadLockGuard guard(segmentAllocLock_);
        if (segmentAlloc_.find(lid) != segmentAlloc_.end()) {
            *alloc = segmentAlloc_[lid];
            return true;
        }

        return false;
    // segmentAlloc_的值不可用，从existSegmentAllocValues_中获取
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
    // segmentAlloc_值不可用,change需要更新到existSegmentAllocValues_中
    if (false == currentValueAvalible_.load()) {
        WriteLockGuard guarg(existSegmentAllocValuesLock_);
        existSegmentAllocValues_[lid] += changeSize;
    }

    // 如果etcd中的数据已经统计结束，直接将change更新到segmentAlloc_中
    if (true == segmentAllocFromEtcdOK_.load()) {
        WriteLockGuard guard(segmentAllocLock_);
        segmentAlloc_[lid] += changeSize;
    // 如果etcd中的数据还未统计结束，将change更新到segmentChange_中
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
    // 从etcd中获取revision之前的alloc数据
    int res;
    do {
        res =  AllocStatisticHelper::CalculateSegmentAlloc(
            curRevision_, client_, &segmentAlloc_);
    } while (HandleResult(res));

    LOG(INFO) << "calculate segment alloc revision not bigger than "
              << curRevision_ << " ok";
    // 设置从etcd中获取数据完成
    segmentAllocFromEtcdOK_.store(true);

    // 这里睡眠5s，极大概率避免merge之后segmentChange_中还有数据
    std::this_thread::sleep_for(std::chrono::seconds(5));

    // 做一次合并
    DoMerge();

    // 设置segmentAlloc_可用
    currentValueAvalible_.store(true);
}

bool AllocStatistic::HandleResult(int res) {
    if (res == 0) {
        return false;
    } else {
        // 获取当前的revision, 如果获取失败，过段时间再尝试
        int errCode = EtcdErrCode::OK;
        do {
            // 如果出错，睡眠一段时间内再做尝试
            if (errCode != EtcdErrCode::OK) {
                LOG(INFO) << "calculateSegmentAlloc"
                          << " occur error, sleep and retry later";
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(retryInterMs_));
            }

            // 1. 清空当前已统计的segmentAlloc_以及更新过来的segmentChange_
            // 2. 重新获取当前的revision
            segmentAlloc_.clear();
            errCode = client_->GetCurrentRevision(&curRevision_);
            if (EtcdErrCode::OK != errCode) {
                LOG(ERROR) << "get current revision error";
            }
        } while (EtcdErrCode::OK != errCode);
        return true;
    }
}

void AllocStatistic::PeriodicPersist() {
    std::map<PoolIdType, int64_t> lastPersist;
    while (sleeper_.wait_for(
        std::chrono::milliseconds(periodicPersistInterMs_))) {
        // 获取本次需要持久化的数据
        std::map<PoolIdType, int64_t> curPersist = GetLatestSegmentAllocInfo();
        if (true == curPersist.empty()) {
            continue;
        }

        // 将curPersist中的值持久化到etcd
        for (auto &item : curPersist) {
            // 如果与上次持久化数据相同，不再重复持久化
            if (lastPersist.find(item.first) != lastPersist.end()) {
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

            if (EtcdErrCode::OK != errCode) {
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
    // 将revision之前的alloc数据和revision之后的数据进行合并
    std::set<PoolIdType> logicalPools = GetCurrentLogicalPools();
    for (PoolIdType lid : logicalPools) {
        UpdateSegmentAllocByCurrrevision(lid);
    }
}

std::map<PoolIdType, int64_t> AllocStatistic::GetLatestSegmentAllocInfo() {
    std::map<PoolIdType, int64_t> curPersist;

    // 如果segmentAlloc_可用，将segmentAlloc_作为本次持久化数据
    if (true == currentValueAvalible_.load()) {
        ReadLockGuard guard(segmentAllocLock_);
        for (auto item : segmentAlloc_) {
            curPersist[item.first] = item.second;
        }
    // 如果segmentAlloc_不可用，将existSegmentAllocValues_作为本次持久化数据
    } else {
        ReadLockGuard guard(existSegmentAllocValuesLock_);
        for (auto item : existSegmentAllocValues_) {
            curPersist[item.first] = item.second;
        }
    }

    return curPersist;
}

void AllocStatistic::UpdateSegmentAllocByCurrrevision(PoolIdType lid) {
    // 获取>revision之后的值
    int64_t sumChangeUntilNow = 0;

    {
        WriteLockGuard guard(segmentChangeLock_);
        // 获取指定lid对应的map
        auto liter = segmentChange_.find(lid);
        if (liter != segmentChange_.end()) {
            // change中的没有数据，可以直接删除，后续直接更新到segment中
            if (liter->second.empty()) {
                segmentChange_.erase(liter);
            } else {
                // 获取>curevisiond的对应的位置
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

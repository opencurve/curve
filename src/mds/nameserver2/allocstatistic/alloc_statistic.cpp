/*
 * Project: curve
 * Created Date: 20190819
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic_helper.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "src/common/concurrent/concurrent.h"

using ::curve::common::Thread;
using ::curve::common::LockGuard;

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
    stop_.store(true);
    periodicPersist_.join();
    calculateAlloc_.join();
}

bool AllocStatistic::GetAllocByLogicalPool(PoolIdType lid, uint64_t *alloc) {
    *alloc = 0;
    bool canReturn = false;
    if (true == currentValueAvalible_.load()) {
        if (segmentAlloc_.find(lid) != segmentAlloc_.end()) {
            *alloc = segmentAlloc_[lid];
        }
        canReturn = true;
    } else if (existSegmentAllocValues_.find(lid) !=
        existSegmentAllocValues_.end()) {
        *alloc = existSegmentAllocValues_[lid];
        canReturn = true;
    }

    if (true == canReturn) {
        LockGuard guard(updateLock);
        if (segmentChange_.find(lid) != segmentChange_.end()) {
            *alloc += segmentChange_[lid];
        }
        return true;
    }

    return false;
}

void AllocStatistic::UpdateChangeLock() {
    updateLock.lock();
}

void AllocStatistic::UpdateChangeUnlock() {
    updateLock.unlock();
}

void AllocStatistic::AllocChange(PoolIdType lid, int64_t changeSize) {
    segmentChange_[lid] += changeSize;
}

void AllocStatistic::CalculateSegmentAlloc() {
    int res;
    do {
        res =  AllocStatisticHelper::CalculateSegmentAlloc(
            curRevision_, client_, &segmentAlloc_);
    } while (HandleResult(res));

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
                LOG(INFO) << "CalculateSegmentAlloc"
                          << " occur error, sleep and retry later";
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(retryInterMs_));
            }

            // 1. 清空当前已统计的segmentAlloc_以及更新过来的segmentChange_
            // 2. 重新获取当前的revision
            {
                LockGuard guard(updateLock);
                segmentChange_.clear();
                segmentAlloc_.clear();
                errCode = client_->GetCurrentRevision(&curRevision_);
                if (EtcdErrCode::OK != errCode) {
                    LOG(ERROR) << "get current revision error";
                }
            }
        } while (EtcdErrCode::OK != errCode);
        return true;
    }
}

void AllocStatistic::PeriodicPersist() {
    std::map<PoolIdType, uint64_t> lastPersit;
    while (false == stop_.load()) {
        if (true == currentValueAvalible_.load()) {
            LOG(INFO) << "periodic persist to etcd start";
            std::map<PoolIdType, uint64_t> res;
            for (auto &item : segmentAlloc_) {
                res[item.first] = item.second;
            }
            {
                LockGuard guard(updateLock);
                for (auto &item : segmentChange_) {
                    res[item.first] += item.second;
                }
            }
            for (auto &item : res) {
                // 如果与上次持久化数据相同，不再重复持久化
                if (lastPersit.find(item.first) != lastPersit.end()) {
                    if (lastPersit[item.first] == item.second) {
                        continue;
                    } else {
                        lastPersit[item.first] = item.second;
                    }
                } else {
                    lastPersit[item.first] = item.second;
                }
                int errCode = client_->Put(
                    NameSpaceStorageCodec::EncodeSegmentAllocKey(item.first),
                    NameSpaceStorageCodec::EncodeSegmentAllocValue(
                        item.first, item.second));
                if (EtcdErrCode::OK != errCode) {
                    LOG(INFO) << "periodic persist to etcd fail, errCode: "
                              << errCode;
                    lastPersit.erase(item.first);
                }
            }
            LOG(INFO) << "periodic persist to etcd end";
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(periodicPersistInterMs_));
    }
}

}  // namespace mds
}  // namespace curve

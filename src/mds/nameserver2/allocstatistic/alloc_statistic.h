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

#ifndef SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_H_
#define SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_H_

#include <memory>
#include <map>
#include <set>
#include <string>
#include "src/kvstorageclient/etcd_client.h"
#include "src/mds/common/mds_define.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"

using ::curve::mds::topology::PoolIdType;
using ::curve::common::Atomic;
using ::curve::common::Mutex;
using ::curve::common::RWLock;
using ::curve::common::Thread;
using ::curve::common::InterruptibleSleeper;

namespace curve {
namespace mds {

using ::curve::kvstorage::EtcdClientImp;

/**
 * AllocStatistic is for counting the number of segments allocated currently
 * The statistics are divided into two parts:
 * part1:
 *     ①statistics of the allocation amount before the designated revision
 *     ②record the segment allocation amount of each revision since mds started
 *     ③combine the data in ① and ②
 * part2: the background periodically persists the merged data in part1
 *
 * maps involved:
 * existSegmentAllocValues_: data persisted in Etcd in the last time mds exited
 *                           + the segment change since mds started
 * segmentAlloc_: store the data after the part1 is merged
 *
 * provide segment allocation data according to current statistical status:
 * 1. If all of part1 are completed, get data from mergeMap_
 * 2. If part1 is not completed, get data from existSegmentAllocValues_
 */

class AllocStatistic {
 public:
    /**
     * @brief UsageStatistic constructor
     *
     * @param[in] periodicPersistInterMs Time interval for persisting the
     *                                   segment usage of each logicalPool in
     *                                   memory to Etcd
     * @param[in] retryInterMs Retry time interval after the failure of getting
     *                         segment of the specified revision from Etcd
     * @param[in] client Etcd client
     */
    AllocStatistic(uint64_t periodicPersistInterMs, uint64_t retryInterMs,
        std::shared_ptr<EtcdClientImp> client) :
        client_(client),
        currentValueAvalible_(false),
        segmentAllocFromEtcdOK_(false),
        stop_(true),
        periodicPersistInterMs_(periodicPersistInterMs),
        retryInterMs_(retryInterMs) {}

    ~AllocStatistic() {
        Stop();
    }

    /**
     * @brief Init Obtains the allocated segment information and information in
     *             recycleBin periodically persisted corresponding to each
     *             physical-pool from Etcd
     *
     * @return 0-init succeeded 1-init failed
     */
    int Init();

   /**
     * @brief Run 1. get all the segments under the specified revision
     *            2. persist the statistics of allocated segment size in memory
     *               under each logicalPool regularly
     */
    void Run();

    /**
     * @brief Stop Waiting for the exit of background threads
     */
    void Stop();

    /**
     * @brief GetAllocByLogicalPool Get the allocated capacity of the specified
     *                             logical pool. If the statistics are not over
     *                              (currentValueAvalible_=false) then use the
     *                              old value, otherwise use the new one
     *
     * @param[in] lid Specifies the logical pool id
     * @param[out] alloc Allocated segment size of lid
     *
     * @return false if acquisition failed, true if succeeded
     */
    virtual bool GetAllocByLogicalPool(PoolIdType lid, int64_t *alloc);

    /**
     * @brief AllocSpace Update after putting segment
     *
     * @param[in] lid ID of the logicalpool in which the segment is located
     * @param[in] changeSize segment increase
     * @param[in] revision Version corresponding to this change
     */
    virtual void AllocSpace(PoolIdType, int64_t changeSize, int64_t revision);

    /**
     * @brief DeAllocSpace Update after deleting segment
     *
     * @param[in] lid LogicalpoolId where the segment is located
     * @param[in] changeSize Segment reduction
     * @param[in] revision Version corresponding to this change
     */
    virtual void DeAllocSpace(
        PoolIdType, int64_t changeSize, int64_t revision);

 private:
     /**
     * @brief CalculateSegmentAlloc Get all the segment records of the
     *                         specified revision from Etcd
     */
    void CalculateSegmentAlloc();

    /**
     * @brief PeriodicPersist Periodically persist the allocated segment size
     *                        data under each logicalPool in RAM
     */
    void PeriodicPersist();

     /**
     * @brief HandleResult Dealing with the situation that error occur when
     *                     obtaining all segment records of specified revision
     */
    bool HandleResult(int res);

    /**
     * @brief DoMerge For each logicalPool, merge the change amount and data read in Etcd //NOLINT
     */
    void DoMerge();

    /**
     * @brief GetLatestSegmentAllocInfo For getting the value needs to be
     *                                  persisted to Etcd currently of each
     *                                  logical pool
     *
     * @return current value needs to be persisted to Etcd of each logicalPool
     */
    std::map<PoolIdType, int64_t> GetLatestSegmentAllocInfo();

    /**
     * @brief UpdateSegmentAllocByCurrrevision Merge the value changes and
     *                                         data read from Etcd
     *
     * @param[in] lid logicalPoolId
     * @param[out] The segment allocation of the logicalpool
     */
    void UpdateSegmentAllocByCurrrevision(PoolIdType lid);

    /**
     * @brief GetCurrentLogicalPools Get all current logicalPool
     *
     * @return logical pool collection
     */
    std::set<PoolIdType> GetCurrentLogicalPools();

 private:
    // etcd module
    std::shared_ptr<EtcdClientImp> client_;

    // revision of the segment currently being counted
    int64_t curRevision_;

    // Last persistent value before mds started
    std::map<PoolIdType, int64_t> existSegmentAllocValues_;
    RWLock existSegmentAllocValuesLock_;

    // At the beginning, stores allocation data of the segment before specified revision //NOLINT
    // Later, stores the merged value
    std::map<PoolIdType, int64_t> segmentAlloc_;
    RWLock segmentAllocLock_;
    Atomic<bool> segmentAllocFromEtcdOK_;

    // Segment changes after mds started
    // PoolIdType: poolId
    // std::map<int64_t, int64_t> first value is the version, and the second is
    //                            the value change
    std::map<PoolIdType, std::map<int64_t, int64_t>> segmentChange_;
    RWLock segmentChangeLock_;

    // Decide whether the value in segmentAlloc_ be used
    // It can be used after at least one merge
    Atomic<bool> currentValueAvalible_;

    // Retry interval in case of error in ms
    uint64_t retryInterMs_;

    // Persistence interval in ms
    uint64_t periodicPersistInterMs_;

    // When stop_ is true, stop the persistent thread and the statistical
    // thread that counts the segment allocation in Etcd
    Atomic<bool> stop_;

    InterruptibleSleeper sleeper_;

    // thread for periodically persisting allocated segment size of each logical pool //NOLINT
    Thread periodicPersist_;

    // thread for calculating allocated segment size under specified revision
    Thread calculateAlloc_;
};
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_ALLOCSTATISTIC_ALLOC_STATISTIC_H_


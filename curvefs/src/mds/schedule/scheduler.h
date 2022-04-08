/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-11-8 11:01:48
 * @Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_SCHEDULE_SCHEDULER_H_
#define CURVEFS_SRC_MDS_SCHEDULE_SCHEDULER_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "curvefs/src/mds/schedule/operator.h"
#include "curvefs/src/mds/schedule/operatorController.h"
#include "curvefs/src/mds/schedule/schedule_define.h"
#include "curvefs/src/mds/schedule/topoAdapter.h"
#include "curvefs/src/mds/topology/topology.h"

namespace curvefs {
namespace mds {
namespace schedule {
using ::curvefs::mds::topology::UNINITIALIZE_ID;
using ::curvefs::mds::topology::PoolIdType;
using ::curvefs::mds::topology::ZoneIdType;
using ::curvefs::mds::topology::ServerIdType;
using ::curvefs::mds::topology::MetaServerIdType;
using ::curvefs::mds::topology::CopySetIdType;

class Scheduler {
 public:
    /**
     * @brief Scheduler constructor
     *
     * @param[in] opt Options
     * @param[in] topo Topology info
     * @param[in] opController Operator management module
     */
    Scheduler(const ScheduleOption &opt,
              const std::shared_ptr<TopoAdapter> &topo,
              const std::shared_ptr<OperatorController> &opController)
        : topo_(topo), opController_(opController) {
        transTimeSec_ = opt.transferLeaderTimeLimitSec;
        removeTimeSec_ = opt.removePeerTimeLimitSec;
        changeTimeSec_ = opt.changePeerTimeLimitSec;
        addTimeSec_ = opt.addPeerTimeLimitSec;
    }

    virtual ~Scheduler() {}

    /**
     * @brief producing operator according to cluster status
     */
    virtual int Schedule();

    /**
     * @brief time interval of generating operations
     */
    virtual int64_t GetRunningInterval() const;

 public:
    /**
     * @brief SelectBestPlacementMetaServer Select a healthy metaserver in
     *                                       the cluster to replace the oldPeer
     *                                       in copysetInfo
     *
     * @param[in] copySetInfo
     * @param[in] copySet Replica to replace
     *
     * @return target metaserver, return UNINITIALIZED if no
     *         metaserver selected
     */
    MetaServerIdType SelectBestPlacementMetaServer(
        const CopySetInfo &copySetInfo, MetaServerIdType oldPeer);

    /**
     * @brief CopysetAllPeersOnline Check whether all replicas of a copyset are
     *        online
     *
     * @param[in] copySetInfo Copyset to check
     *
     * @return true if all online, false if there's any offline replica
     */
    bool CopysetAllPeersOnline(const CopySetInfo &copySetInfo);

 protected:
    std::shared_ptr<TopoAdapter> topo_;
    // operator management module
    std::shared_ptr<OperatorController> opController_;

    // maximum estimated time of transferring the leader, alarm if exceeded
    int transTimeSec_;
    // maximum estimated tim for adding peers, alarm if exceeded
    int addTimeSec_;
    // maximum estimated time for removing peer, alarm if exceeded
    int removeTimeSec_;
    // maximum estimated time for changing peer, alarm if exceeded
    int changeTimeSec_;
};

// recovering the offline replicas
class RecoverScheduler : public Scheduler {
 public:
    RecoverScheduler(const ScheduleOption &opt,
                     const std::shared_ptr<TopoAdapter> &topo,
                     const std::shared_ptr<OperatorController> &opController)
        : Scheduler(opt, topo, opController) {
        runInterval_ = opt.recoverSchedulerIntervalSec;
    }

    /**
     * @brief recovering the offline replica on Topology
     *
     * @return the number of operators generated
     */
    int Schedule() override;

    /**
     * @brief running time interval of the scheduler
     *
     * @return time interval
     */
    int64_t GetRunningInterval() const override;

 private:
    /**
     * @brief fix the specified replica
     *
     * @param[in] info The copyset to be fixed
     * @param[in] peerId The metaserver(replica) to be fixed
     * @param[out] op The operator generated
     * @param[out] target The replica added
     *
     * @return Whether any operator has been generated
     */
    bool FixOfflinePeer(const CopySetInfo &info, MetaServerIdType peerId,
                        Operator *op, MetaServerIdType *target);

 private:
    // running interval of RecoverScheduler
    int64_t runInterval_;
};

class CopySetScheduler : public Scheduler {
 public:
    CopySetScheduler(const ScheduleOption &opt,
                     const std::shared_ptr<TopoAdapter> &topo,
                     const std::shared_ptr<OperatorController> &opController)
        : Scheduler(opt, topo, opController) {
        runInterval_ = opt.copysetSchedulerIntervalSec;
        balanceRatioPercent_ = opt.balanceRatioPercent;
    }

    /**
     * @brief Schedule Generating operator according to
     *        the condition of the cluster
     *
     * @return operator num generated
     */
    int Schedule() override;

    /**
     * @brief get running interval of CopySetScheduler
     *
     * @return time interval
     */
    int64_t GetRunningInterval() const override;

 private:
    /**
     * @brief CopySetScheduleForPool Operate copyset balancing on
     *        specified pool
     *
     * @param[in] poolId Specified pool id
     *
     * @return Source node of the migration, for test
     */
    int CopySetScheduleForPool(PoolIdType poolId);

    void GetCopySetDistribution(
        const std::vector<CopySetInfo> &copysetList,
        const std::vector<MetaServerInfo> &metaserverList,
        std::map<MetaServerIdType, std::vector<CopySetInfo>> *out);

    bool TransferCopyset(const CopySetInfo &copyset, MetaServerIdType sourceId,
                         MetaServerIdType destId);

    bool IsCopysetCanTransfer(const CopySetInfo &copyset);

    int CopySetScheduleOverloadMetaserver(PoolIdType poolId);

    int CopySetScheduleNormalMetaserver(PoolIdType poolId);

    int CheckAndBalanceZone(ZoneIdType zoneId);

    void FileterUnhealthyMetaserver(
        std::vector<MetaServerInfo> *metaserverVector);

 private:
    // Running interval of CopySetScheduler
    int64_t runInterval_;

    uint32_t balanceRatioPercent_;
};

class LeaderScheduler : public Scheduler {
 public:
    LeaderScheduler(
        const ScheduleOption &opt,
        const std::shared_ptr<TopoAdapter> &topo,
        const std::shared_ptr<OperatorController> &opController)
        : Scheduler(opt, topo, opController) {
        runInterval_ = opt.leaderSchedulerIntervalSec;
        metaserverCoolingTimeSec_ = opt.metaserverCoolingTimeSec;
    }

    /**
     * @brief Schedule Generate operators according to the status of the cluster
     *
     * @return number of operators generated
     */
    int Schedule() override;

    /**
     * @brief Get running interval of LeaderScheduler
     *
     * @return time interval
     */
    int64_t GetRunningInterval() const override;

    // for test
    bool GetRapidLeaderSchedulerFlag();
    void SetRapidLeaderSchedulerFlag(bool flag);

 private:
    int LeaderSchedulerForPool(PoolIdType poolId);

    bool TransferLeaderOut(MetaServerIdType source, uint16_t replicaNum,
        PoolIdType poolId, Operator *op, CopySetInfo *selectedCopySet);

    bool TransferLeaderIn(MetaServerIdType target, uint16_t replicaNum,
        PoolIdType poolId, Operator *op, CopySetInfo *selectedCopySet);

    bool CopySetHealthy(const CopySetInfo &csInfo);

    /**
     * @brief CoolingTimeExpired Check whether current-time - aliveTime is
     *                           larger than metaserverCoolingTimeSec_
     *
     * @brief aliveTime The running time of the metaserver
     *
     * @return false if current-time - aliveTime <= metaserverCoolingTimeSec_
     *         true if not
     */
    bool CoolingTimeExpired(uint64_t aliveTime);

 private:
    int64_t runInterval_;

    // the minimum time that a metaserver can become a target
    // leader after it started
    uint32_t metaserverCoolingTimeSec_;

    // retry times of method transferLeaderout
    const int maxRetryTransferLeader = 10;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SCHEDULE_SCHEDULER_H_

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
 * Created Date: Thu Nov 15 2018
 * Author: lixiaocui
 */

#ifndef SRC_MDS_SCHEDULE_SCHEDULER_H_
#define SRC_MDS_SCHEDULE_SCHEDULER_H_

#include <utility>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <memory>
#include <set>
#include "src/mds/schedule/schedule_define.h"
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/schedule/operatorController.h"
#include "src/mds/topology/topology.h"

using ::curve::mds::topology::UNINTIALIZE_ID;
using ::curve::mds::topology::LogicalPoolIdType;
using ::curve::mds::topology::PhysicalPoolIdType;
using ::curve::mds::topology::ZoneIdType;
using ::curve::mds::topology::ServerIdType;
using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::CopySetIdType;


namespace curve {
namespace mds {
namespace schedule {

struct LeaderStatInLogicalPool {
    // id of current leader
    PoolIdType lid;
    // average number of leaders on every chunkserver in current logical pool
    int avgLeaderNum;
    // copyset distribution of chunkservers on current logical pool
    std::map<ChunkServerIdType, std::vector<CopySetInfo>> distribute;
    // leader number of every chunkser in current logical pool
    std::map<ChunkServerIdType, int> leaderNumInChunkServer;
};

class Scheduler {
 public:
    /**
     * @brief Scheduler constructor
     *
     * @param[in] opt Options
     * @param[in] topo Topology info
     * @param[in] opController Operator management module
     */
    Scheduler(
        const ScheduleOption &opt,
        const std::shared_ptr<TopoAdapter> &topo,
        const std::shared_ptr<OperatorController> &opController)
        : topo_(topo), opController_(opController) {
        transTimeSec_ = opt.transferLeaderTimeLimitSec;
        removeTimeSec_ = opt.removePeerTimeLimitSec;
        changeTimeSec_ = opt.changePeerTimeLimitSec;
        addTimeSec_ = opt.addPeerTimeLimitSec;
        scanTimeSec_ = opt.scanPeerTimeLimitSec;
        scatterWidthRangePerent_ = opt.scatterWithRangePerent;
    }

    /**
     * @brief producing operator according to cluster status
     */
    virtual int Schedule();

    /**
     * @brief time interval of generating operations
     */
    virtual int64_t GetRunningInterval();

 protected:
    /**
     * @brief SelectBestPlacementChunkServer Select a healthy chunkserver in
     *                                       the cluster to replace the oldPeer
     *                                       in copysetInfo
     *
     * @param[in] copySetInfo
     * @param[in] copySet Replica to replace
     *
     * @return target chunkserver, return UNINITIALIZED if no
     *         chunkserver selected
     */
    ChunkServerIdType SelectBestPlacementChunkServer(
        const CopySetInfo &copySetInfo, ChunkServerIdType oldPeer);

    /**
     * @brief SelectRedundantReplicaToRemove select a replica from
     *        copyset to remove
     *
     * @param[in] copySetInfo
     *
     * @return replica to remove, return UNINITIALIZED if no
     *         chunkserver selected
     */
    ChunkServerIdType SelectRedundantReplicaToRemove(
        const CopySetInfo &copySetInfo);

    /**
     * @brief GetMinScatterWidth Get minimum value of scatter width according to
     *                           the result calculated by average value and
     *                           the percentage (scatterWidthRangePerent_)
     *
     * @param[in] lpid Logical pool id
     *
     * @return mimimum Scatter-width
     */
    int GetMinScatterWidth(PoolIdType lpid);

    /**
     * @brief CopysetAllPeersOnline Check whether all replicas of a copyset are online //NOLINT
     *
     * @param[in] copySetInfo Copyset to check
     *
     * @return true if all online, false if there's any offline replica
     */
    bool CopysetAllPeersOnline(const CopySetInfo &copySetInfo);

 protected:
    // scatter width of chunkserver should be equal or smaller than
    // (1 + minScatterWdith_) * scatterWidthRangePerent_
    float scatterWidthRangePerent_;

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
    // maximum estimated time for start/cancel scan peer, alarm if exceeded
    int scanTimeSec_;
};

// scheduler for balancing copyset number and chunkserver scatter width
class CopySetScheduler : public Scheduler {
 public:
    CopySetScheduler(
        const ScheduleOption &opt,
        const std::shared_ptr<TopoAdapter> &topo,
        const std::shared_ptr<OperatorController> &opController)
        : Scheduler(opt, topo, opController) {
        runInterval_ = opt.copysetSchedulerIntervalSec;
        copysetNumRangePercent_ = opt.copysetNumRangePercent;
    }

    /**
     * @brief Schedule Generating operator according to
     *        the condition of the cluster
     *
     * @return chunkserver to add, a value for POC
     */
    int Schedule() override;

    /**
     * @brief get running interval of CopySetScheduler
     *
     * @return time interval
     */
    int64_t GetRunningInterval() override;

 private:
    /**
     * @brief DoCopySetSchedule Operate copyset balancing on
     *        specified logical pool
     *
     * @param[in] lid Specified logical pool id
     *
     * @return Source node of the migration, for test only
     */
    int DoCopySetSchedule(PoolIdType lid);

    /**
     * @brief StatsCopysetDistribute Calculate the average number, range and
     *        standard deviation of copyset on chunkserver
     *
     * @param[in] distribute Copyset on every chunkservers
     * @param[out] avg
     * @param[out] range
     * @param[out] standard deviation
     */
    void StatsCopysetDistribute(
        const std::map<ChunkServerIdType, std::vector<CopySetInfo>> &distribute,
        float *avg, int *range, float *stdvariance);

    /**
     * @brief CopySetMigration Select a copyset according to current copyset
     *                         distribution on Topology, and specify the source
     *                         and target
     *
     * @param[in] chunkserverlist Every chunkservers in Topology,
     *                            for avoiding duplicate fetching as a parameter
     * @param[in] distribute Copyset on every chunkserver
     * @param[out] op Operator generated
     * @param[out] source The chunkserver specified to remove copyset
     * @param[out] target The chunkserver specified to add copyset
     * @param[out] choose Copyset chosen
     *
     * @return true if operator generated false if not
     */
    bool CopySetMigration(
        const std::map<ChunkServerIdType, std::vector<CopySetInfo>> &distribute,
        Operator *op, ChunkServerIdType *source, ChunkServerIdType *target,
        CopySetInfo *choose);

    /**
     * @brief CopySetSatisfiyBasicMigrationCond To check whether the copyset
     *                                          specified fulfill the
     *                                          requirements below:
     *                                          1. no changes running
     *                                          2. has same amount of replicas
     *                                             as the standard
     *                                          3. topology initialization
     *                                             finished
     *                                          4. every replicas of copyset
     *                                             are online
     *
     * @param[info] info Copyset specified
     *
     * @return true if every conditions are fulfilled,
     *         false if any of it does not
     */
    bool CopySetSatisfiyBasicMigrationCond(const CopySetInfo &info);

 private:
    // Running interval of CopySetScheduler
    int64_t runInterval_;

    // can be changed according to the scatter width of the initial status
    // of the cluster
    // the range of the copyset number on chunkserver can not exceed
    // (avg * copysetNumRangePercent_), avg is the average number of copyset
    // on chunkserver
    float copysetNumRangePercent_;
};

// Scheduler for balancing the leader number
class LeaderScheduler : public Scheduler {
 public:
    LeaderScheduler(
        const ScheduleOption &opt,
        const std::shared_ptr<TopoAdapter> &topo,
        const std::shared_ptr<OperatorController> &opController)
        : Scheduler(opt, topo, opController) {
        runInterval_ = opt.leaderSchedulerIntervalSec;
        chunkserverCoolingTimeSec_ = opt.chunkserverCoolingTimeSec;
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
    int64_t GetRunningInterval() override;

 private:
    /**
     * @brief Select a leader copyset randomly on the source chunkserver,
     *        and migrate the leader out
     *
     * @param[in] source The ID of the chunkserver
     *            where leader migration executed
     * @param[in] leaderCount The number of the leaders of source chunkserver
     * @param[in] lid The ID of the logical pool under balancing
     * @param[out] op The operator generated
     * @param[out] selectedCopySet The selected copyset to change
     *
     * @return return true if operator generated, false if not
     */
    bool transferLeaderOut(ChunkServerIdType source, int leaderCount,
        PoolIdType lid, Operator *op, CopySetInfo *selectedCopySet);

    /**
     * @brief Select a follower copyset randomly on target chunkserver, and
     *        migrate the leader to this chunkserver
     *
     * @param[in] target The ID of the chunkserver that the leader will
     *            be migrated to
     * @param[in] leaderCount The number of the leader on target chunkserver
     * @param[in] lid The ID of the logical pool under balancing
     * @param[out] op The operator generated
     * @param[out] selectedCopySet The selected copyset to change
     *
     * @return return true if the operator generated successfully, false if not
     */
    bool transferLeaderIn(ChunkServerIdType target, int leaderCount,
        PoolIdType lid, Operator *op, CopySetInfo *selectedCopySet);

    /**
    * @brief copySetHealthy Check the online status of three replicas
    *
    * @param[in] csInfo Copyset Info
    *
    * @return true if none of them is offline, false if any
    */
    bool copySetHealthy(const CopySetInfo &csInfo);

    /**
     * @brief coolingTimeExpired Check whether current-time - aliveTime is
     *                           larger than chunkserverCoolingTimeSec_
     *
     * @brief aliveTime The running time of the chunkserver
     *
     * @return false if current-time - aliveTime <= chunkserverCoolingTimeSec_
     *         true if not
     */
    bool coolingTimeExpired(uint64_t aliveTime);

    /**
     * @brief DoLeaderSchedule Execute leader balancing to
     *        specified logical pool
     *
     * @param[in] lid The ID of the logical pool specified
     *
     * @return The number of the effective operator generated
     */
    int DoLeaderSchedule(PoolIdType lid);

 private:
    int64_t runInterval_;

    // the minimum time that a chunkserver can become a target
    // leader after it started
    uint32_t chunkserverCoolingTimeSec_;

    // retry times of method transferLeaderout
    const int maxRetryTransferLeader = 10;
};

// recovering the offline replicas
class RecoverScheduler : public Scheduler {
 public:
    RecoverScheduler(
        const ScheduleOption &opt,
        const std::shared_ptr<TopoAdapter> &topo,
        const std::shared_ptr<OperatorController> &opController)
        : Scheduler(opt, topo, opController) {
        runInterval_ = opt.recoverSchedulerIntervalSec;
        chunkserverFailureTolerance_ = opt.chunkserverFailureTolerance;
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
    int64_t GetRunningInterval() override;

 private:
    /**
     * @brief fix the specified replica
     *
     * @param[in] info The copyset to be fixed
     * @param[in] peerId The chunkserver(replica) to be fixed
     * @param[out] op The operator generated
     * @param[out] target The replica added
     *
     * @return Whether any operator has been generated
     */
    bool FixOfflinePeer(const CopySetInfo &info, ChunkServerIdType peerId,
        Operator *op, ChunkServerIdType *target);

    /**
     * @brief calculate the number of the chunkserver that has offline
     *        replicas more than a specific number on a server. for those
     *        server, the chunkserver on it will not be recovered.
     *
     * @param[out] excludes Chunkservers on the server that has offline
     *                      Chunkserver more than a specified number
     */
    void CalculateExcludesChunkServer(std::set<ChunkServerIdType> *excludes);

 private:
    // running interval of RecoverScheduler
    int64_t runInterval_;
    // the threshold of the failing chunkserver that the server will not be recovered //NOLINT
    int32_t chunkserverFailureTolerance_;
};

// Check replica numbers of the copyset according to the configuration, and
// remove or add replica if the number didn't satisfy the requirement
class ReplicaScheduler : public Scheduler {
 public:
    ReplicaScheduler(
        const ScheduleOption &opt,
        const std::shared_ptr<TopoAdapter> &topo,
        const std::shared_ptr<OperatorController> &opController)
        : Scheduler(opt, topo, opController) {
        runInterval_ = opt.replicaSchedulerIntervalSec;
    }

    /**
     * @brief Schedule Check whether the replica numebr of the copyset satisfies
     *                 the standard, and generate operator for adjustment if not
     *
     * @return the number of operators generated
     */
    int Schedule() override;

    /**
     * @brief get running time interval of the scheduler
     *
     * @return time interval
     */
    int64_t GetRunningInterval() override;

 private:
    // time interval of replicaScheduler
    int64_t runInterval_;
};

// for rapid leader balancing
class RapidLeaderScheduler : public Scheduler {
 public:
    RapidLeaderScheduler(
        const ScheduleOption &opt,
        const std::shared_ptr<TopoAdapter> &topo,
        const std::shared_ptr<OperatorController> &opController,
        PoolIdType lpid)
        : Scheduler(opt, topo, opController), lpoolId_(lpid) {}

    /**
     * @brief Execute leader balancing in a granularity of logical pool
     * @return kScheduleErrCodeSuccess create some operators of transferleader
     *                                 successfully
     */
    int Schedule() override;

 private:
    /**
     * @brief Execute leader balancing in specified logical pool
     * @param[in] lid The logical pool specified
     */
    void DoRapidLeaderSchedule(LogicalPoolIdType lid);

    /**
     * @brief measure the info of leader distribution on specified logical pool
     * @param[out] stat The result of the measurement
     * @return false if failed to initialize, true if succeeded
     */
    bool LeaderStatInSpecifiedLogicalPool(LeaderStatInLogicalPool *stat);

    /**
     * @brief select possible target leader node for a copyset
     * @param[in] curChunkServerId The ID of the chunkserver under processing
     * @param[in] info The specified copyset
     * @param[in] stat The distribution statistic of leaders
     * @return fChunkServerIdType
     */
    ChunkServerIdType SelectTargetPeer(ChunkServerIdType curChunkServerId,
        const CopySetInfo &info, const LeaderStatInLogicalPool &stat);

    /**
     * @brief find the chunkserver that has the minimum number of leaders in peers of a copyset //NOLINT
     * @param[in] info The copyset specified
     * @param[in] stat The info of the distribution of leaders
     * @return The ID of the chunkserver that has the minimum number of leaders
     */
    ChunkServerIdType MinLeaderNumInCopySetPeers(
        const CopySetInfo &info, const LeaderStatInLogicalPool &stat);

    /**
     * @brief decide whether a node can be the new leader
     *        criterion:
     *        1. the variance of the leader number from source node and target
     *           node is larger than 1
     *        2. current leader number of the source node is larger than average
     * @param[in] origLeader The old leader
     * @param[in] targetLeader Potentail target node as the new leader
     * @param[in] stat The info of the distribution of leaders
     * @return true if the target node is able to be the new leader, false if not //NOLINT
     */
    bool PossibleTargetPeerConfirm(ChunkServerIdType origLeader,
        ChunkServerIdType targetLeader, const LeaderStatInLogicalPool &stat);

    /**
     * @brief generate operator for transferleader for specified copyset
     * @param[in] info The copyset specified
     * @param[in] targetLeader Target leader node
     * @return true if generated successfully, false if not
     */
    bool GenerateLeaderChangeOperatorForCopySet(
        const CopySetInfo &info, ChunkServerIdType targetLeader);

 private:
    PoolIdType lpoolId_;
};

class ScanScheduler : public Scheduler {
 public:
    using CopySetInfos = std::vector<CopySetInfo>;
    using Selected = std::unordered_map<ChunkServerIdType, int>;

 public:
    ScanScheduler(
        const ScheduleOption &opt,
        const std::shared_ptr<TopoAdapter> &topo,
        const std::shared_ptr<OperatorController> &opController)
        : Scheduler(opt, topo, opController) {
        runInterval_ = opt.scanSchedulerIntervalSec;
        scanStartHour_ = opt.scanStartHour;
        scanEndHour_ = opt.scanEndHour;
        scanIntervalSec_ = opt.scanIntervalSec;
        scanConcurrentPerPool_ = opt.scanConcurrentPerPool;
        scanConcurrentPerChunkserver_ = opt.scanConcurrentPerChunkserver;
    }

    /**
     * @brief Schedule Generate operators according to the status of the cluster
     * @return number of operators generated
     */
    int Schedule() override;

    /**
     * @brief Get running interval of ScanScheduler
     * @return time interval
     */
    int64_t GetRunningInterval() override;

 private:
    /**
     * @brief Check whether the specify copyset is start/ready to scan
     * @param[in] copysetInfo the specify copyset
     * @return true if the copyset is start/ready to scan, else return false
     */
    bool StartOrReadyToScan(const CopySetInfo& copysetInfo);

    /**
     * @brief Select copysets to start scan
     * @param[in] copysetInfos copysets to be selected
     * @param[in] count the number of copysets should be selected
     * @param[in] selected the selected chunkserver id
     * @param[out] copysets2start the selected copysets
     */
    void SelectCopysetsToStartScan(CopySetInfos* copysetInfos,
                                   int count,
                                   Selected* selected,
                                   CopySetInfos* copysets2start);

    /**
     * @brief Select copysets to cancel scan
     * @param[in] copysetInfos copysets to be selected
     * @param[in] count the number of copysets should be selected
     * @param[out] copysets2cancel the selected copysets
     */
    void SelectCopysetsToCancelScan(CopySetInfos* copysetInfos,
                                    int count,
                                    CopySetInfos* copysets2cancel);

    /**
     * @brief Select copysets to start/cancel scan
     * @param[in] copysetInfos copysets to be selected
     * @param[out] copysets2start the selected copysets to start scan
     * @param[out] copysets2cancel the selected copysets to cancel scan
     */
    void SelectCopysetsForScan(const CopySetInfos& copysetInfos,
                               CopySetInfos* copysets2start,
                               CopySetInfos* copysets2cancel);

    /**
     * @brief Generate operator for the specify copysets
     * @param[in] copysetInfos copysets need to generate operator
     * @param[in] opType operator type, support
     *            (ConfigChangeType::[START|CANCEL]_SCAN_PEER)
     * @return the number of operators generated
     */
    int GenScanOperator(const CopySetInfos& copysetInfos,
                        ConfigChangeType opType);

 private:
    // scan scheduler run interval
    uint32_t runInterval_;

    // scan start hour in one day ([0-23])
    uint32_t scanStartHour_;

    // scan end hour in one day ([0-23])
    uint32_t scanEndHour_;

    // scan interval for the same copyset (seconds)
    uint32_t scanIntervalSec_;

    // maximum number of scan copysets at the same time for every logical pool
    uint32_t scanConcurrentPerPool_;

    // maximum number of scan copysets at the same time for every chunkserver
    uint32_t scanConcurrentPerChunkserver_;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULER_H_

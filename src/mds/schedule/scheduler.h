/*
 * Project: curve
 * Created Date: Thu Nov 15 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_SCHEDULE_SCHEDULER_H_
#define SRC_MDS_SCHEDULE_SCHEDULER_H_

#include <utility>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <set>
#include "src/mds/schedule/schedule_define.h"
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/schedule/operatorController.h"
#include "src/mds/topology/topology.h"

namespace curve {
namespace mds {
namespace schedule {

struct LeaderStatInLogicalPool {
    // 当前leader id
    PoolIdType lid;
    // 当前逻辑池中每个chunkserver上平均leader数量
    int avgLeaderNum;
    // 当前逻辑池中chunkserver上copyset的分布
    std::map<ChunkServerIdType, std::vector<CopySetInfo>> distribute;
    // 当前逻辑池中每个chunkserver上leader的数量
    std::map<ChunkServerIdType, int> leaderNumInChunkServer;
};

class Scheduler {
 public:
    /**
     * @brief Scheduler构造函数
     *
     * @param[in] opt 配置项
     * @param[in] topo 提供拓扑逻辑信息
     * @param[in] opController operator管理模块
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
        scatterWidthRangePerent_ = opt.scatterWithRangePerent;
    }

    /**
     * @brief scheduler根据集群的状况产生operator
     */
    virtual int Schedule();

    /**
     * @brief operator产生的时间间隔，单位是秒
     */
    virtual int64_t GetRunningInterval();

 protected:
    /**
     * @brief SelectBestPlacementChunkServer 从集群中选择一个健康的chunkserver
     *        替换copySetInfo中的oldPeer
     *
     * @param[in] copySetInfo copyset信息
     * @param[in] copySet需要被替换的副本
     *
     * @return 目的chunkserver, 如果为UNINITIALIZED表示未选出
     */
    ChunkServerIdType SelectBestPlacementChunkServer(
        const CopySetInfo &copySetInfo, ChunkServerIdType oldPeer);

    /**
     * @brief SelectRedundantReplicaToRemove 从copyset的副本中选择一个移除
     *
     * @param[in] copySetInfo copyset信息
     *
     * @return 要移除的副本，如果为UNINITIALIZED表示未选出
     */
    ChunkServerIdType SelectRedundantReplicaToRemove(
        const CopySetInfo &copySetInfo);

    /**
     * @brief GetMinScatterWidth 根据均值和百分比获取scatter-width的设定最小值
     *
     * @param[in] lpid 逻辑池id
     *
     * @return scatter-width的设定最小值
     */
    int GetMinScatterWidth(PoolIdType lpid);

     /**
     * @brief CopysetAllPeersOnline 指定copyset的所有副本是否在线
     *
     * @param[in] copySetInfo 指定copyset
     *
     * @return true-所有副本都在线 false-有副本不在线
     */
    bool CopysetAllPeersOnline(const CopySetInfo &copySetInfo);

 protected:
    // chunkserver的scatter-width不能超过
    // (1 + minScatterWdith_) * scatterWidthRangePerent_
    float scatterWidthRangePerent_;

    std::shared_ptr<TopoAdapter> topo_;
    // operator管理模块
    std::shared_ptr<OperatorController> opController_;

    // transfer leader的最大预计时间，超过需要报警
    int transTimeSec_;
    // add peer的最大预计时间，超过需要报警
    int addTimeSec_;
    // remove peer的最大预计时间，超过需要报警
    int removeTimeSec_;
    // change peer的最大预计时间，超过需要报警
    int changeTimeSec_;
};

// copyset数量和chunkserver scatter-with均衡
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
     * @brief Schedule根据集群的状况产生operator
     *
     * @return 需要增加的chunkserverId, 这个返回值是为了POC进行处理
     */
    int Schedule() override;

    /**
     * @brief 获取CopySetScheduler的运行间隔
     *
     * @return 时间间隔
     */
    int64_t GetRunningInterval() override;

 private:
    /**
     * @brief DoCopySetSchedule 对指定的logicalPool做copyset均衡
     *
     * @param[in] lid 指定逻辑池id
     *
     * @return 本次迁移的源节点，仅供测试使用
     */
    int DoCopySetSchedule(PoolIdType lid);

    /**
     * @brief StatsCopysetDistribute
     *        计算chunkserver上copyset数量的均值、极差、标准差
     *
     * @param[in] distribute 每个chunkserver上的copyset
     * @param[out] avg 均值
     * @param[out] range 极差
     * @param[out] stdvariance 标准差
     */
    void StatsCopysetDistribute(
        const std::map<ChunkServerIdType, std::vector<CopySetInfo>> &distribute,
        float *avg, int *range, float *stdvariance);

    /**
     * @brief CopySetMigration
     *        根据当前topo中copyset的分布选择一个copyset, 确定source和target
     *
     * @param[in] chunkserverlist topo中所有chunkserver, 作为参数是为了避免重复获取
     * @param[in] distribute 每个chunkserver上的copyset
     * @param[out] op 生成的operator
     * @param[out] source 需要移除copyset的chunkserver
     * @param[out] target 需要增加copyset的chunkserver
     * @param[out] choose 选中的copyset
     *
     * @return true-生成operator false-未生成operator
     */
    bool CopySetMigration(
        const std::map<ChunkServerIdType, std::vector<CopySetInfo>> &distribute,
        Operator *op, ChunkServerIdType *source, ChunkServerIdType *target,
        CopySetInfo *choose);

    /**
     * @brief CopySetSatisfiyBasicMigrationCond
     *        指定copyset是否符合下列条件：
     *        1. copyset上没有正在进行的变更
     *        2. copyset的副本数目与标准一致
     *        3. topology初始化已完成
     *        4. copyset的所有副本均在线
     *
     * @param[info] info 指定copyset
     *
     * @return true-符合所有条件 false-其中有条件不符合
     */
    bool CopySetSatisfiyBasicMigrationCond(const CopySetInfo &info);

 private:
    // CopySetScheduler运行时间间隔
    int64_t runInterval_;

    // 相关配置, 可以根据集群初始状态的scatterwith设置
    // chunkserver上copyset数量的极差不能超过均值百分比
    float copysetNumRangePercent_;
};

// leader数量均衡
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
     * @brief Schedule根据集群的状况产生operator
     *
     * @return 产生operator的个数
     */
    int Schedule() override;

    /**
     * @brief 获取LeaderScheduler的运行间隔
     *
     * @return 时间间隔
     */
    int64_t GetRunningInterval() override;

 private:
    /**
     * @brief 在source上随机选择一个leader copyset, 把leader从该chunkserver
     *        上迁移出去
     *
     * @param[in] source leader需要迁移出去的chunkserverID
     * @param[in] leaderCount source上leader的个数
     * @param[in] lid 当前正在均衡的logicalPoolId
     * @param[out] op 生成的operator
     * @param[out] selectedCopySet 选中的需要变更的copyset
     *
     * @return 是否成功生成operator, false为没有生成
     */
    bool transferLeaderOut(ChunkServerIdType source, int leaderCount,
        PoolIdType lid, Operator *op, CopySetInfo *selectedCopySet);

    /**
     * @brief 在target上随机选择一个follower copyset, 把leader迁移到该chunserver上
     *
     * @param[in] target 需要将该leader迁移到该chunkserverID
     * @param[in] leaderCount target上leader的个数
     * @param[in] 当前正在均衡的逻辑池id
     * @param[out] op 生成的operator
     * @param[out] selectedCopySet 选中的需要变更的copyset
     *
     * @return 是否成功生成operator, false为没有生成
     */
    bool transferLeaderIn(ChunkServerIdType target, int leaderCount,
        PoolIdType lid, Operator *op, CopySetInfo *selectedCopySet);

    /*
    * @brief copySetHealthy检查copySet三个副本是否都在线
    *
    * @param[in] csInfo copyset的信息
    *
    * @return false为三个副本至少有一个不在线， true为三个副本均为online状态
    */
    bool copySetHealthy(const CopySetInfo &csInfo);

    /**
     * @brief coolingTimeExpired 判断当前时间 - aliveTime
     *                           是否大于chunkserverCoolingTimeSec_
     *
     * @brief aliveTime chunkserver的启动时间
     *
     * @return false表示 当前时间 - aliveTime <= chunkserverCoolingTimeSec_
     *         true 当前时间 - aliveTime > chunkserverCoolingTimeSec_
     */
    bool coolingTimeExpired(uint64_t aliveTime);

    /**
     * @brief DoLeaderSchedule 对指定的logicalPool做leader均衡
     *
     * @param[in] lid 指定逻辑池id
     *
     * @return 本次均衡产生的有效operator个数
     */
    int DoLeaderSchedule(PoolIdType lid);

 private:
    int64_t runInterval_;

    // chunkserver启动coolingTimeSec_后才可以作为target leader
    uint32_t chunkserverCoolingTimeSec_;

    // transferLeaderout的重试次数
    const int maxRetryTransferLeader = 10;
};

// 用于修复offline的副本
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
     * @brief 修复topology中offline的副本
     *
     * @return 生成的operator的数量
     */
    int Schedule() override;

    /**
     * @brief scheduler运行的时间间隔
     *
     * @return 时间间隔
     */
    int64_t GetRunningInterval() override;

 private:
    /**
     * @brief 修复指定副本
     *
     * @param[in] info 待修复的copyset
     * @param[in] peerId 待修复的副本
     * @param[out] op 生成的operator
     * @param[out] target 新增副本
     *
     * @return 是否生成了operator
     */
    bool FixOfflinePeer(const CopySetInfo &info, ChunkServerIdType peerId,
        Operator *op, ChunkServerIDType *target);

    /**
     * @brief 统计server上有哪些offline超过一定数量的chunkserver集合
     *
     * @param[out] excludes server上offlinechunkserver超过一定数量的chunkserver集合//NOLINT
     */
    void CalculateExcludesChunkServer(std::set<ChunkServerIdType> *excludes);

 private:
    // RecoverScheduler运行间隔
    int64_t runInterval_;
    // 一个Server上超过offlineExceed_个chunkserver挂掉,不恢复
    int32_t chunkserverFailureTolerance_;
};

// 根据配置检查copyset的副本数量, 副本数量不符合标准值时进行删除或增加
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
     * @brief Schedule检查copyset的副本数量是否符合标准值, 如果不符合, 生成operator //NOLINT
     *        调整副本数量
     *
     * @return 生成的operator的数量
     */
    int Schedule() override;

    /**
     * @brief scheduler运行的时间间隔
     *
     * @return 时间间隔
     */
    int64_t GetRunningInterval() override;

 private:
    // replicaScheduler运行间隔
    int64_t runInterval_;
};

// 快速leader均衡
class RapidLeaderScheduler : public Scheduler {
 public:
    RapidLeaderScheduler(
        const ScheduleOption &opt,
        const std::shared_ptr<TopoAdapter> &topo,
        const std::shared_ptr<OperatorController> &opController,
        PoolIdType lpid)
        : Scheduler(opt, topo, opController), lpoolId_(lpid) {}

    /**
     * @brief 以逻辑池为力度做leader均衡
     * @return 返回值是一个:
     *         kScheduleErrCodeSuccess 成功生成了一批transferleader的operator
     */
    int Schedule() override;

 private:
    /**
     * @brief 在指定逻辑池中做leader均衡
     * @param[in] lid 指定logicalpool
     */
    void DoRapidLeaderSchedule(LogicalPoolIDType lid);

    /**
     * @brief 统计指定逻辑池中leader分布的信息
     * @param[out] stat 统计结果
     * @return false-初始化失败 true-初始化成功
     */
    bool LeaderStatInSpecifiedLogicalPool(LeaderStatInLogicalPool *stat);

    /**
     * @brief 为指定copyset选择有可能的目的leader节点
     * @param[in] curChunkServerId 当前正在处理的chunkserver
     * @param[in] info 指定复制组
     * @param[in] stat leader的分布的统计信息
     * @return fChunkServerIdType
     */
    ChunkServerIdType SelectTargetPeer(ChunkServerIdType curChunkServerId,
        const CopySetInfo &info, const LeaderStatInLogicalPool &stat);

    /**
     * @brief copyset的各副本中，leader数目最小的副本
     * @param[in] info 指定复制组
     * @param[in] stat leader的分布的统计信息
     * @return leader数目最小的副本id
     */
    ChunkServerIdType MinLeaderNumInCopySetPeers(
        const CopySetInfo &info, const LeaderStatInLogicalPool &stat);

    /**
     * @brief 判断潜在目的节点是否可以作为新leader
     * 标准：1. 源节点和目的节点上leader的数量差大于1
     *      2. 源节点上当前leader的数量大于均值
     * @param[in] origLeader 旧leader
     * @param[in] targetLeader 潜在目的节点
     * @param[in] stat leader的分布的统计信息
     * @return true-可以作为新leader false-不可以作为新leader
     */
    bool PossibleTargetPeerConfirm(ChunkServerIdType origLeader,
        ChunkServerIdType targetLeader, const LeaderStatInLogicalPool &stat);

    /**
     * @brief 为指定copyset生成transferleader的operator
     * @param[in] info 指定复制组
     * @param[in] targetLeader 目的节点
     * @return true-生成成功  false-生成失败
     */
    bool GenerateLeaderChangeOperatorForCopySet(
        const CopySetInfo &info, ChunkServerIdType targetLeader);

 private:
    PoolIdType lpoolId_;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULER_H_

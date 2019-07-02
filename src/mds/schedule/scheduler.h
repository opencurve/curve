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
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/schedule/operator.h"
#include "src/mds/schedule/operatorController.h"
#include "src/mds/topology/topology.h"

namespace curve {
namespace mds {
namespace schedule {

enum SchedulerType {
  LeaderSchedulerType,
  CopySetSchedulerType,
  RecoverSchedulerType,
  ReplicaSchedulerType,
};

class Scheduler {
 public:
    /**
     * @brief Scheduler构造函数
     *
     * @param[in] transTimeLimitSec leader变更mds端认为的超时时间
     * @param[in] removeTimeLimitSec 减一个副本mds端认为的超时时间
     * @param[in] addTimeLimtSec 增加一个副本mds端认为的超时时间
     * @param[in] scatterWithRangePerent scatter-width不能超过
     *            (1 + scatterWithRangePerent) * minScatterWdith
     * @param[in] topo 提供拓扑逻辑信息
     * @param[in] opController operator管理模块
     */
    Scheduler(int transTimeLimitSec, int removeTimeLimitSec, int addTimeLimtSec,
        float scatterWidthRangePerent,
        const std::shared_ptr<TopoAdapter> &topo,
        const std::shared_ptr<OperatorController> &opController) {
        this->transTimeSec_ = transTimeLimitSec;
        this->removeTimeSec_ = removeTimeLimitSec;
        this->addTimeSec_ = addTimeLimtSec;
        this->scatterWidthRangePerent_ = scatterWidthRangePerent;
        this->topo_ = topo;
        this->opController_ = opController;
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
};

// copyset数量和chunkserver scatter-with均衡
class CopySetScheduler : public Scheduler {
 public:
    /**
     * @brief CopySetScheduler
     *
     * @param[in] opController 管理operator
     * @param[in] interSec CopySetScheduler运行时间间隔, 单位是秒
     * @param[in] transTimeLimitSec leader变更mds端认为的超时时间, 父函数初始化需要 //NOLINT
     * @param[in] removeTimeLimitSec 减一个副本mds端认为的超时时间, 父函数初始化需要 //NOLINT
     * @param[in] addTimeLimtSec 增加一个副本mds端认为的超时时间, 父函数初始化需要 //NOLINT
     * @param[in] scatterWithRangePerent scatter-width不能超过
     *            (1 + scatterWithRangePerent) * minScatterWdith, 父函数初始化需要 //NOLINT
     * @param[in] copysetNumRangePercent [chunkserver上copyset数量的极差]不能超过 //NOLINT
     *             [chunkserver上copyset数量均值] * copysetNumRangePercent
     * @param[in] topo 提供拓扑逻辑信息, 父函数初始化需要 // NOLINIT
     */
    CopySetScheduler(const std::shared_ptr<OperatorController> &opController,
                    int64_t interSec,
                    int transTimeLimitSec,
                    int removeTimeLimitSec,
                    int addTimeLimitSec,
                    float copysetNumRangePercent,
                    float scatterWithRangePerent,
                    const std::shared_ptr<TopoAdapter> &topo)
        : Scheduler(transTimeLimitSec, removeTimeLimitSec, addTimeLimitSec,
            scatterWithRangePerent, topo, opController) {
        this->runInterval_ = interSec;
        this->copysetNumRangePercent_ = copysetNumRangePercent;
    }

    /**
     * @brief Schedule根据集群的状况产生operator
     *
     * @return 需要增加的chunkserverId, 这个返回值是为了POC进行处理
     */
    int Schedule() override;

    /**
     *  @brief 获取CopySetScheduler的运行间隔
     *
     * @return 时间间隔
     */
    int64_t GetRunningInterval() override;

 private:
    /**
     * @brief CopySetDistribution 统计online状态chunkserver上的copyset
     *
     * @param[in] copysetList topology中所有copyset
     * @param[in] chunkserverList topology中所有chunkserver
     * @param[out] out chunkserver对应的copyset列表
     */
    void CopySetDistributionInOnlineChunkServer(
        const std::vector<CopySetInfo> &copysetList,
        const std::vector<ChunkServerInfo> &chunkserverList,
        std::map<ChunkServerIdType, std::vector<CopySetInfo>> *out);

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
    /**
     * @brief LeaderScheduler
     *
     * @param[in] opController 管理operator
     * @param[in] interSec LeaderScheduler运行时间间隔
     * @param[in] transTimeLimitSec leader变更mds端认为的超时时间, 父函数初始化需要 //NOLINT
     * @param[in] removeTimeLimitSec 减一个副本mds端认为的超时时间, 父函数初始化需要 //NOLINT
     * @param[in] addTimeLimitSec 增加一个副本mds端认为的超时时间, 父函数初始化需要 //NOLINT
     * @param[in] scatterWithRangePerent scatter-width不能超过
     *            (1 + scatterWithRangePerent) * minScatterWdith, 父函数初始化需要 //NOLINT
     * @param[in] topo 提供拓扑逻辑信息, 父函数初始化需要 // NOLINIT
     */
    LeaderScheduler(const std::shared_ptr<OperatorController> &opController,
                    int64_t interSec,
                    int transTimeLimitSec,
                    int addTimeLimitSec,
                    int removeTimeLimitSec,
                    float scatterWidthRangePerent,
                    const std::shared_ptr<TopoAdapter> &topo)
        : Scheduler(transTimeLimitSec, removeTimeLimitSec, addTimeLimitSec,
            scatterWidthRangePerent, topo, opController) {
        this->runInterval_ = interSec;
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
     * @param[out] op 生成的operator
     *
     * @return 是否成功生成operator, -1为没有生成
     */
    int transferLeaderOut(ChunkServerIdType source, Operator *op);

    /**
     * @brief 在target上随机选择一个follower copyset, 把leader迁移到该chunserver上
     *
     * @param[in] target 需要将该leader迁移到该chunkserverID
     * @param[out] op 生成的operator
     *
     * @return 是否成功生成operator, -1为没有生成
     */
    int transferLeaderIn(ChunkServerIdType target, Operator *op);

    /*
    * @brief copySetHealthy检查copySet三个副本是否都在线
    *
    * @param[in] csInfo copyset的信息
    *
    * @return false为三个副本至少有一个不在线， true为三个副本均为online状态
    */
    bool copySetHealthy(const CopySetInfo &csInfo);

 private:
    int64_t runInterval_;

    // transferLeaderout的重试次数
    const int maxRetryTransferLeader = 10;
};

// 用于修复offline的副本
class RecoverScheduler : public Scheduler {
 public:
    RecoverScheduler(const std::shared_ptr<OperatorController> &opController,
                    int64_t interSec,
                    int transTimeLimitSec,
                    int removeTimeLimitSec,
                    int addTimeLimitSec,
                    float scatterWithRangePerent,
                    int chunkserverFailureTolerance,
                    const std::shared_ptr<TopoAdapter> &topo)
        : Scheduler(transTimeLimitSec, removeTimeLimitSec, addTimeLimitSec,
            scatterWithRangePerent, topo, opController) {
        this->runInterval_ = interSec;
        this->chunkserverFailureTolerance_ = chunkserverFailureTolerance;
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
    /**
     * @brief LeaderScheduler
     *
     * @param[in] opController 管理operator
     * @param[in] interSec LeaderScheduler运行时间间隔
     * @param[in] transTimeLimitSec leader变更mds端认为的超时时间, 父函数初始化需要 //NOLINT
     * @param[in] removeTimeLimitSec 减一个副本mds端认为的超时时间, 父函数初始化需要 //NOLINT
     * @param[in] addTimeLimitSec 增加一个副本mds端认为的超时时间, 父函数初始化需要 //NOLINT
     * @param[in] scatterWidthRangePerent scatter-width不能超过
     *            (1 + scatterWidthRangePerent) * minScatterWdith, 父函数初始化需要 //NOLINT
     * @param[in] minScatterWdith 最小scatter-width, 父函数初始化需要 //NOLINT
     * @param[in] topo 提供拓扑逻辑信息, 父函数初始化需要 // NOLINIT
     */
    ReplicaScheduler(const std::shared_ptr<OperatorController> &opController,
                   int64_t interSec,
                   int transTimeLimitSec,
                   int removeTimeLimitSec,
                   int addTimeLimitSec,
                   float scatterWidthRangePerent,
                   const std::shared_ptr<TopoAdapter> &topo)
      : Scheduler(transTimeLimitSec, removeTimeLimitSec, addTimeLimitSec,
        scatterWidthRangePerent, topo, opController) {
    this->opController_ = opController;
    this->runInterval_ = interSec;
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
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULER_H_

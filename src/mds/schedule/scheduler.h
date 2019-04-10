/*
 * Project: curve
 * Created Date: Thu Nov 15 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_SCHEDULE_SCHEDULER_H_
#define SRC_MDS_SCHEDULE_SCHEDULER_H_

#include <string>
#include <vector>
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
  Scheduler(int transTimeLimitSec, int removeTimeLimitSec, int addTimeLimtSec);
  /**
   * @brief scheduler根据集群的状况产生operator
   *
   * @param[in] topo 获取集群信息
   */
  virtual int Schedule(
      const std::shared_ptr<TopoAdapter> &topo);

  /**
   * @brief operator产生的时间间隔，单位是秒
   */
  virtual int64_t GetRunningInterval();

 protected:
  /*
  * @brief 获取transfer leader的时间限制
  * 
  * @return 时间，单位秒
  */
  int GetTransferLeaderTimeLimitSec();

  /**
   * @brief 获取add peer的时间限制
   *
   * @return 时间，单位为秒
   */
  int GetAddPeerTimeLimitSec();

  /**
   * @brief 获取remove peer的时间限制
   * 
   * @return 时间，单位为秒
   */
  int GetRemovePeerTimeLimitSec();

 private:
  // transfer leader的最大预计时间，超过需要报警
  int transTimeSec_;

  // add peer的最大预计时间，超过需要报警
  int addTimeSec_;

  // remove peer的最大预计时间，超过需要报警
  int removeTimeSec_;
};

// TODO(lixiaocui): implement
/**
 * @brief 容量均衡
 */
class CopySetScheduler : public Scheduler {
 public:
  CopySetScheduler(const std::shared_ptr<OperatorController> &opController,
                   int64_t inter,
                   int transTimeLimitSec,
                   int removeTimeLimitSec,
                   int addTimeLimitSec)
      : Scheduler(transTimeLimitSec, removeTimeLimitSec, addTimeLimitSec) {}

  int Schedule(const std::shared_ptr<TopoAdapter> &topo) override;
  int64_t GetRunningInterval() override;

 private:
  std::shared_ptr<OperatorController> opController_;
  int64_t runInterval_;
};


/**
 * @brief leader数量均衡
 */
class LeaderScheduler : public Scheduler {
 public:
  LeaderScheduler(const std::shared_ptr<OperatorController> &opController,
                  int64_t inter,
                  int transTimeLimitSec,
                  int addTimeLimitSec,
                  int removeTimeLimitSec)
      : Scheduler(transTimeLimitSec, removeTimeLimitSec, addTimeLimitSec) {
      this->opController_ = opController;
      this->runInterval_ = inter;
      }

  int Schedule(const std::shared_ptr<TopoAdapter> &topo) override;

  int64_t GetRunningInterval() override;

 private:
  /**
   * @brief 在source上随机选择一个leader copyset, 把leader从该chunkserver
   *        上迁移出去
   *
   * @param[in] source leader需要迁移出去的chunkserverID
   * @param[in] topo 用于获取集群信息
   * @param[out] op 生成的operator
   *
   * @return 是否成功生成operator, -1为没有生成
   */
  int transferLeaderOut(ChunkServerIdType source,
                        const std::shared_ptr<TopoAdapter> &topo,
                        Operator *op);

  /**
   * @brief 在target上随机选择一个follower copyset, 把leader迁移到该chunserver上
   *
   * @param[in] target 需要将该leader迁移到该chunkserverID
   * @param[in] topo 用于获取集群信息
   * @param[out] op 生成的operator
   *
   * @return 是否成功生成operator, -1为没有生成
   */
  int transferLeaderIn(ChunkServerIdType target,
                       const std::shared_ptr<TopoAdapter> &topo,
                       Operator *op);

  /*
  * @brief copySetHealthy检查copySet三个副本是否都在线
  *
  * @param[in] csInfo copyset的信息
  * @param[in] topo 用于获取copyset上三个副本的状态
  *
  * @return false为三个副本至少有一个不在线， true为三个副本均为online状态
  */
  bool copySetHealthy(
      const CopySetInfo &csInfo, const std::shared_ptr<TopoAdapter> &topo);

 private:
  std::shared_ptr<OperatorController> opController_;
  int64_t runInterval_;

  // transferLeaderout的重试次数
  const int maxRetryTransferLeader = 10;
};

/**
 * @brief 用于修复offline的副本
 */
class RecoverScheduler : public Scheduler {
 public:
  RecoverScheduler(const std::shared_ptr<OperatorController> &opController,
                   int64_t inter,
                   int transTimeLimitSec,
                   int removeTimeLimitSec,
                   int addTimeLimitSec)
      : Scheduler(transTimeLimitSec, removeTimeLimitSec, addTimeLimitSec) {
      this->opController_ = opController;
      this->runInterval_ = inter;
  }

  /**
   * @brief 修复topology中offline的副本
   *
   * @param[in] topo 获取集群状态
   *
   * @return 生成的operator的数量
   */
  int Schedule(const std::shared_ptr<TopoAdapter> &topo) override;

  /**
   * @brief scheduler运行的时间间隔
   */
  int64_t GetRunningInterval() override;

 private:
  /**
   * @brief 修复指定副本
   *
   * @param[in] topo 用于获取集群状态
   * @param[in] info 待修复的copyset
   * @param[in] peerId 待修复的副本
   * @param[out] 生成的operator
   *
   * @return 是否生成了operator
   */
  bool FixOfflinePeer(const std::shared_ptr<TopoAdapter> &topo,
                      const CopySetInfo &info,
                      ChunkServerIdType peerId,
                      Operator *op);

 private:
  std::shared_ptr<OperatorController> opController_;
  int64_t runInterval_;
};

/**
 * @brief 根据配置检查copyset的副本数量
 */
class ReplicaScheduler : public Scheduler {
 public:
  ReplicaScheduler(const std::shared_ptr<OperatorController> &opController,
                   int64_t inter,
                   int transTimeLimitSec,
                   int removeTimeLimitSec,
                   int addTimeLimitSec)
      : Scheduler(transTimeLimitSec,
                  removeTimeLimitSec,
                  addTimeLimitSec) {
                      this->opController_ = opController;
                      this->runInterval_ = inter;
                  }

  int Schedule(const std::shared_ptr<TopoAdapter> &topo) override;
  int64_t GetRunningInterval() override;

 private:
  std::shared_ptr<OperatorController> opController_;
  int64_t runInterval_;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULER_H_

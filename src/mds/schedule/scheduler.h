/*
 * Project: curve
 * Created Date: Thu Nov 15 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_SCHEDULE_SCHEDULER_H_
#define CURVE_SRC_MDS_SCHEDULE_SCHEDULER_H_

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
   * @brief scheduler generate operators according to the
   * current state of cluster, called by background thread coordinator run.
   */
  virtual int Schedule(
      const std::shared_ptr<TopoAdapter> &topo);

  /**
   * @brief operator-generate interval, the unit is second.
   */
  virtual int64_t GetRunningInterval();

  int GetTransferLeaderTimeLimitSec();

  int GetAddPeerTimeLimitSec();

  int GetRemovePeerTimeLimitSec();

 private:
  int transTimeSec_;
  int addTimeSec_;
  int removeTimeSec_;
};

// TODO(lixiaocui): implement
/**
 * @brief capacity rebalance
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

// TODO(lixiaocui): implement
/**
 * @brief leaderNum rebalance
 */
class LeaderScheduler : public Scheduler {
 public:
  LeaderScheduler(const std::shared_ptr<OperatorController> &opController,
                  int64_t inter,
                  int transTimeLimitSec,
                  int addTimeLimitSec,
                  int removeTimeLimitSec)
      : Scheduler(transTimeLimitSec, removeTimeLimitSec, addTimeLimitSec) {}

  int Schedule(const std::shared_ptr<TopoAdapter> &topo) override;

  int64_t GetRunningInterval() override;

 private:
  std::shared_ptr<OperatorController> opController_;
  int64_t runInterval_;
};

/**
 * @brief repair the offline replica
 */
class RecoverScheduler : public Scheduler {
 public:
  /**
   * @brief RecoverScheduler constructor
   *
   * @param opController: shared with schedule module
   * @param inter: running interval
   */
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
   * @brief  schedule calculate and generate operators according to
   * the current state of topo
   */
  int Schedule(const std::shared_ptr<TopoAdapter> &topo) override;

  /**
   * @brief get running interval
   */
  int64_t GetRunningInterval() override;

 private:
  /**
   * @brief  called by CheckOfflinePeer to repair specified offline replica
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
 * @brief check if the num of copySet replica is equal to conf
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
                  addTimeLimitSec) {}

  int Schedule(const std::shared_ptr<TopoAdapter> &topo) override;
  int64_t GetRunningInterval() override;

 private:
  std::shared_ptr<Operator> *opController_;
  int64_t runInterval_;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // CURVE_SRC_MDS_SCHEDULE_SCHEDULER_H_

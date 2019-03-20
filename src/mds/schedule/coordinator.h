/*
 * Project: curve
 * Created Date: Thu Nov 16 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#ifndef CURVE_SRC_MDS_SCHEDULE_COORDINATOR_H_
#define CURVE_SRC_MDS_SCHEDULE_COORDINATOR_H_

#include <vector>
#include <map>
#include <thread>  //NOLINT
#include <boost/shared_ptr.hpp>
#include "src/mds/dao/mdsRepo.h"
#include "src/mds/schedule/operatorController.h"
#include "src/mds/schedule/scheduler.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/schedule/topoAdapter.h"

using ::curve::mds::heartbeat::ConfigChangeType;

namespace curve {
namespace mds {
namespace schedule {
// TODO(chaojie-schedule): needTopoTocontain
struct ScheduleConfig {
 public:
  ScheduleConfig(bool enableCopySet,
                 bool enableLeader,
                 bool enableRecover,
                 bool enableReplica,
                 int64_t copySetInerval,
                 int64_t leaderInterval,
                 int64_t recoverInterval,
                 int64_t replicaInterval,
                 int opConcurrent,
                 int tranferLimit,
                 int removeLimit,
                 int addLimit) {
      this->EnableCopysetScheduler = enableCopySet;
      this->EnableLeaderScheduler = enableLeader;
      this->EnableRecoverScheduler = enableRecover;
      this->EnableReplicaScheduler = enableReplica;
      this->CopysetSchedulerInterval = copySetInerval;
      this->ReplicaSchedulerInterval = replicaInterval;
      this->LeaderSchedulerInterval = leaderInterval;
      this->RecoverSchedulerInterval = recoverInterval;
      this->OperatorConcurrent = opConcurrent;
      this->TransferLeaderTimeLimitSec = tranferLimit;
      this->RemovePeerTimeLimitSec = removeLimit;
      this->AddPeerTimeLimitSec = addLimit;
  }
  bool EnableCopysetScheduler;
  bool EnableLeaderScheduler;
  bool EnableRecoverScheduler;
  bool EnableReplicaScheduler;

  // scheduler calculate interval
  int64_t CopysetSchedulerInterval;
  int64_t LeaderSchedulerInterval;
  int64_t RecoverSchedulerInterval;
  int64_t ReplicaSchedulerInterval;

  // para
  int OperatorConcurrent;
  int TransferLeaderTimeLimitSec;
  int AddPeerTimeLimitSec;
  int RemovePeerTimeLimitSec;
};

class Coordinator {
 public:
  Coordinator() = default;
  explicit Coordinator(const std::shared_ptr<TopoAdapter> &topo);
  ~Coordinator();

  /**
   * @brief 处理chunkServer上报的copySet信息
   *
   * @param originInfo 入参,心跳传递过来的copySet信息
   * @param newConf    出参, 处理过后返还给chunkServer的copyset信息
   *
   * @return if newConf is assigned return true else return false
   */
  virtual bool CopySetHeartbeat(const CopySetInfo &originInfo,
                                CopySetConf *newConf);

  /**
   * @brief 根据配置初始化scheduler
   *
   * @param conf 入参, sheduler配置信息
   */
  void InitScheduler(const ScheduleConfig &conf);

  /**
   * @brief run schedulers in background threads according to scheduleConf
   */
  void Run();

  /**
   * @brief stop scheduler threads
   */
  void Stop();

  // TODO(lixiaocui): 对外接口,根据运维需求增加
  /**
   * @brief configChange for admin
   *
   * @param id: cpoysetID
   * @param type: config change type: transfer-leader/add-peer/remove-peer
   *
   * @param item: config change operation item
   */
  void DoConfigChange(CopySetKey id,
                      ConfigChangeType type,
                      ChunkServerIdType item);

  /**
    * @brief for unit test
    */
  std::shared_ptr<OperatorController> GetOpController();

 private:
  /**
   * @brief set scheduleRunning_, if set false, stop schedule threads
   *
   * @param flag
   */
  void SetSchedulerRunning(bool flag);

  /**
   * @brief 定时任务, 运行不同的scheduler
   *
   * @param s run schedule at interval
   */
  void RunScheduler(const std::shared_ptr<Scheduler> &s);

 private:
  // TODO(lixiaocui): operator persistence, may be donnot need
  // std::shared_ptr<::curve::repo::Repo> repo_;
  std::shared_ptr<TopoAdapter> topo_;

  std::map<SchedulerType, std::shared_ptr<Scheduler>> schedulerController_;
  std::map<SchedulerType, std::thread> runSchedulerThreads_;
  std::shared_ptr<OperatorController> opController_;

  bool schedulerRunning_;
  std::mutex mutex_;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif   // CURVE_SRC_MDS_SCHEDULE_COORDINATOR_H_

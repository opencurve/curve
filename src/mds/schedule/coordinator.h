/*
 * Project: curve
 * Created Date: Thu Nov 16 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#ifndef SRC_MDS_SCHEDULE_COORDINATOR_H_
#define SRC_MDS_SCHEDULE_COORDINATOR_H_

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
     * @param[in] originInfo 心跳传递过来的copySet信息
     * @param[out] newConf   处理过后返还给chunkServer的copyset信息
     *
     * @return if newConf is assigned return true else return false
     */
    virtual bool CopySetHeartbeat(
        const ::curve::mds::topology::CopySetInfo &originInfo,
        ::curve::mds::heartbeat::CopysetConf *newConf);

    /**
     * @brief 根据配置初始化scheduler
     *
     * @param[in] conf, scheduler配置信息
     */
    void InitScheduler(const ScheduleConfig &conf);

    /**
     * @brief 根据scheduler的配置在后台运行各种scheduler
     */
    void Run();

    /**
     * @brief 停止scheduler的后台线程
     */
    void Stop();

    // TODO(lixiaocui): 对外接口,根据运维需求增加
    /**
     * @brief 给管理员提供的接口
     *
     * @param[in] id: cpoysetID
     * @param[in] type: 配置变更类型: transfer-leader/add-peer/remove-peer
     * @param[in] item: 变更项. tansfer-leader时是新leader的id, add-peer时是
     *                  add target, remove-peer时是removew target
     */
    void DoConfigChange(CopySetKey id,
                        ConfigChangeType type,
                        ChunkServerIdType item);

    /**
    * @brief 提供给单元测试使用
    */
    std::shared_ptr<OperatorController> GetOpController();

 private:
    /**
     * @brief  SetScheduleRunning，如果设置为false,则停止所有的scheduelr
     *
     * @param[in] flag 为false，所有sheduler会停止
     */
    void SetSchedulerRunning(bool flag);

    /**
     * @brief 定时任务, 运行不同的scheduler
     *
     * @param[in] s 定时运行scheduler
     */
    void RunScheduler(const std::shared_ptr<Scheduler> &s);

 private:
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

#endif  // SRC_MDS_SCHEDULE_COORDINATOR_H_

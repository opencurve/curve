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
#include <memory>
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
struct ScheduleOption {
 public:
    // copyset均衡的开关
    bool enableCopysetScheduler;
    // leader均衡开关
    bool enableLeaderScheduler;
    // recover开关
    bool enableRecoverScheduler;
    // replica开关
    bool enableReplicaScheduler;

    // copyset均衡计算的时间间隔
    uint32_t copysetSchedulerIntervalSec;
    // leader均衡计算时间间隔
    uint32_t leaderSchedulerIntervalSec;
    // recover计算时间间隔
    uint32_t recoverSchedulerIntervalSec;
    // replica均衡时间间隔
    uint32_t replicaSchedulerIntervalSec;

    // 单个chunkserver上面可以同时进行配置变更的copyset数量
    uint32_t operatorConcurrent;
    // leader变更时间限制, 大于该时间mds认为超时，移除相关operator
    uint32_t transferLeaderTimeLimitSec;
    // 增加节点时间限制, 大于该时间mds认为超时，移除相关operator
    uint32_t addPeerTimeLimitSec;
    // 移除节点时间限制, 大于该时间mds认为超时，移除相关operator
    uint32_t removePeerTimeLimitSec;

    // 供copysetScheduler使用, [chunkserver上copyset数量的极差]不能超过
    // [chunkserver上copyset数量均值] * copysetNumRangePercent
    float copysetNumRangePercent;
    // 配置变更需要尽量使得chunkserver的scatter-with不超过
    // minScatterWith * (1 + scatterWidthRangePerent)
    float scatterWithRangePerent;
    // 一个Server上超过offlineExceed_个chunkserver挂掉,不恢复
    uint32_t chunkserverFailureTolerance;
    // chunkserver启动coolingTimeSec_后才可以作为leader均衡中的target leader
    // chunkserver刚启时copyset会回放日志, 而transferleader的时候会停止现在的io,
    // 如果作为目标leader,就需要等待日志回放完成才可以接受transferleader，回放时间
    // 过长，就导致leadertimeout时间内io会被卡住
    uint32_t chunkserverCoolingTimeSec;
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
     * @return 如果有新的配置生成，返回candaidate Id，如果没有返回UNINTIALIZE_ID
     */
    virtual ChunkServerIdType CopySetHeartbeat(
        const ::curve::mds::topology::CopySetInfo &originInfo,
        const ::curve::mds::heartbeat::ConfigChangeInfo &configChInfo,
        ::curve::mds::heartbeat::CopySetConf *newConf);

    /**
     * @brief ChunkserverGoingToAdd 判断指定chunkserver是否为指定copyset上
     *                              已有AddOperator的target
     *
     * @param[in] csId 指定chunkserver
     * @param[in] key 指定chunserver
     */
    virtual bool ChunkserverGoingToAdd(ChunkServerIdType csId, CopySetKey key);

    /**
     * @brief 根据配置初始化scheduler
     *
     * @param[in] conf, scheduler配置信息
     */
    void InitScheduler(const ScheduleOption &conf);

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

    /**
     * @brief BuildCopySetConf 构建下发给chunkserver的copyset配置
     *
     * @param[in] res applyOperator的结果
     * @param[out] 处理过后返还给chunkServer的copyset信息
     *
     * @return true-构建成功 false-构建失败
     */
    bool BuildCopySetConf(
        const CopySetConf &res, ::curve::mds::heartbeat::CopySetConf *out);

 private:
    std::shared_ptr<TopoAdapter> topo_;

    std::map<SchedulerType, std::shared_ptr<Scheduler>> schedulerController_;
    std::map<SchedulerType, common::Thread> runSchedulerThreads_;
    std::shared_ptr<OperatorController> opController_;

    bool schedulerRunning_;
    std::mutex mutex_;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_COORDINATOR_H_

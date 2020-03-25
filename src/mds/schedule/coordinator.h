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
#include <string>
#include <thread>  //NOLINT
#include <boost/shared_ptr.hpp>
#include "src/mds/dao/mdsRepo.h"
#include "src/mds/schedule/operatorController.h"
#include "src/mds/schedule/scheduler.h"
#include "src/mds/schedule/schedule_define.h"
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/schedule/scheduleMetrics.h"
#include "src/common/interruptible_sleeper.h"

using ::curve::mds::heartbeat::ConfigChangeType;
using ::curve::common::InterruptibleSleeper;

namespace curve {
namespace mds {
namespace schedule {

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
     * @brief 处理快速leader均衡的请求
     *
     * @param[in] lpid 需要进行快速leader均衡的logicalpool
     *
     * @return 返回值有两种:
     *         kScheduleErrCodeSuccess 成功生成了一批transferleader的operator
     *         kScheduleErrCodeInvalidLogicalPool 指定logicalpool在集群中不存在
     */
    virtual int RapidLeaderSchedule(PoolIdType lpid);

    /**
     * @brief 判断指定chunkserver是否为指定copyset上
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
     * @param[in] metrics, scheduleMetric用于operator增加/减少时进行统计
     */
    void InitScheduler(
        const ScheduleOption &conf, std::shared_ptr<ScheduleMetrics> metrics);

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
     * @param[in] type scheduler类型
     */
    void RunScheduler(const std::shared_ptr<Scheduler> &s, SchedulerType type);

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

    /**
     * @brief ScheduleNeedRun 指定类型的调度器是否允许运行
     *
     * @param[in] type 调度器类型
     *
     * @return true-允许运行, false-不允许运行
     */
    bool ScheduleNeedRun(SchedulerType type);

    /**
     * @brief ScheduleName 指定类型的调度器的名字
     *
     * @param[in] type 调度器类型
     *
     * @return 调度器名字
     */
    std::string ScheduleName(SchedulerType type);

 private:
    std::shared_ptr<TopoAdapter> topo_;
    ScheduleOption conf_;

    std::map<SchedulerType, std::shared_ptr<Scheduler>> schedulerController_;
    std::map<SchedulerType, std::thread> runSchedulerThreads_;
    std::shared_ptr<OperatorController> opController_;

    InterruptibleSleeper sleeper_;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_COORDINATOR_H_

/*
 * Project: curve
 * Created Date: Thu Nov 16 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_SCHEDULE_SCHEDULE_DEFINE_H_
#define SRC_MDS_SCHEDULE_SCHEDULE_DEFINE_H_

namespace curve {
namespace mds {
namespace schedule {

enum SchedulerType {
  LeaderSchedulerType,
  CopySetSchedulerType,
  RecoverSchedulerType,
  ReplicaSchedulerType,
  RapidLeaderSchedulerType,
};

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
    // change节点时间限制，大于该时间mds认为超时，移除相关operator
    uint32_t changePeerTimeLimitSec;

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

}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULE_DEFINE_H_

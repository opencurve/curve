/*
 * Project: curve
 * Created Date: 20190704
 * Author: lixiaocui
 * Copyright (c) 2019 netease
 */

#ifndef SRC_MDS_SCHEDULE_SCHEDULERMETRIC_H_
#define SRC_MDS_SCHEDULE_SCHEDULERMETRIC_H_

#include <bvar/bvar.h>
#include <string>

namespace curve {
namespace mds {
namespace schedule {

class SchedulerMetric {
 public:
    // 实现单例
    static SchedulerMetric *GetInstance();

    // 构造函数
    SchedulerMetric() :
    operatorNum(SchedulerMetricPrefix, "operator_num"),
    addOpNum(SchedulerMetricPrefix, "addPeer_num"),
    removeOpNum(SchedulerMetricPrefix, "removePeer_num"),
    transferOpNum(SchedulerMetricPrefix, "transferLeader_num"),
    normalOpNum(SchedulerMetricPrefix, "normal_operator_num"),
    highOpNum(SchedulerMetricPrefix, "high_operator_num") {}

 public:
    const std::string SchedulerMetricPrefix = "mds_scheduler_metric_";

    // 正在执行的所有operator的数量
    bvar::Adder<uint32_t> operatorNum;
    // 正在执行的AddPeer operator的数量
    bvar::Adder<uint32_t> addOpNum;
    // 正在执行的RemovePeer operator的数量
    bvar::Adder<uint32_t> removeOpNum;
    // 正在执行的TransferLeader operator的数量
    bvar::Adder<uint32_t> transferOpNum;
    // 正在执行的normal级别的operator的数量
    bvar::Adder<uint32_t> normalOpNum;
    // 正在执行的high级别的operator的数量
    bvar::Adder<uint32_t> highOpNum;

 private:
    static SchedulerMetric* self_;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULERMETRIC_H_


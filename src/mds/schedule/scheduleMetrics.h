/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 20190704
 * Author: lixiaocui
 */

#ifndef SRC_MDS_SCHEDULE_SCHEDULEMETRICS_H_
#define SRC_MDS_SCHEDULE_SCHEDULEMETRICS_H_

#include <bvar/bvar.h>
#include <string>
#include <map>
#include <memory>
#include "src/mds/schedule/operatorController.h"
#include "src/common/stringstatus.h"

using ::curve::mds::heartbeat::ConfigChangeType;
using ::curve::common::StringStatus;

namespace curve {
namespace mds {
namespace schedule {
extern const char ADDPEER[];
extern const char REMOVEPEER[];
extern const char TRANSFERLEADER[];
extern const char CHANGEPEER[];
extern const char NORMAL[];
extern const char HIGH[];

class ScheduleMetrics {
 public:
    // 构造函数
    explicit ScheduleMetrics(std::shared_ptr<Topology> topo) :
    operatorNum(ScheduleMetricsPrefix, "operator_num"),
    addOpNum(ScheduleMetricsPrefix, "addPeer_num"),
    removeOpNum(ScheduleMetricsPrefix, "removePeer_num"),
    transferOpNum(ScheduleMetricsPrefix, "transferLeader_num"),
    changeOpNum(ScheduleMetricsPrefix, "changePeer_num"),
    normalOpNum(ScheduleMetricsPrefix, "normal_operator_num"),
    highOpNum(ScheduleMetricsPrefix, "high_operator_num"),
    topo_(topo) {}

    /**
     * @brief UpdateAddMetric 暴露给operatorContoller的接口,
     *                        用于增加operator的时候更新metric
     *
     * @param[in] op, 具体的operator
     */
    void UpdateAddMetric(const Operator &op);

    /**
     * @brief UpdateMetric 暴露给operatorContoller的接口,
     *                     用于减少operator的时候更新metric
     *
     * @param[in] op, 具体的operator
     */
    void UpdateRemoveMetric(const Operator &op);

 private:
    /**
     * @brief GetHostNameAndPortById 获取指定chunkserverId的hostName:port
     *
     * @param[in] csid, 指定的chunkserver id
     *
     * @return hostName:port的表现形式
     */
    std::string GetHostNameAndPortById(ChunkServerIdType csid);

    /**
     * @brief GetOpPriorityStr 获取operator优先级的string形式
     *
     * @param[in] pri, 指定优先级
     *
     * @return 优先级对应的string形式
     */
    std::string GetOpPriorityStr(OperatorPriority pri);

    /**
     * @brief RemoveUpdateOperatorsMap remove operator时更新operator map
     *
     * @param[in] op, 具体operator
     * @param[in] type, operator类型，包含AddPeer/RemovePeer/TransferLeader
     * @param[in] target, 变更对象
     */
    void RemoveUpdateOperatorsMap(
        const Operator &op, std::string type, ChunkServerIdType target);

    /**
     * @brief AddUpdateOperatorsMap add operator时更新operator map
     *
     * @param[in] op, 具体operator
     * @param[in] type, operator类型，包含AddPeer/RemovePeer/TransferLeader
     * @param[in] target, 变更对象
     */
    void AddUpdateOperatorsMap(
        const Operator &op, std::string type, ChunkServerIdType target);

    /**
     * @brief UpdateOperatorsMap 构造需要导出的operator相关内容，并转化为json格式
     *
     * @param[in] op, 具体operator
     * @param[in] type, operator类型，包含AddPeer/RemovePeer/TransferLeader
     * @param[in] target, 变更对象
     */
    void UpdateOperatorsMap(
        const Operator &op, std::string type, ChunkServerIdType target);

 public:
    const std::string ScheduleMetricsPrefix = "mds_scheduler_metric_";
    const std::string ScheduleMetricsCopySetOpPrefix =
        "mds_scheduler_metric_copyset_";

    // 正在执行的所有operator的数量
    bvar::Adder<uint32_t> operatorNum;
    // 正在执行的AddPeer operator的数量
    bvar::Adder<uint32_t> addOpNum;
    // 正在执行的RemovePeer operator的数量
    bvar::Adder<uint32_t> removeOpNum;
    // 正在执行的TransferLeader operator的数量
    bvar::Adder<uint32_t> transferOpNum;
    // 正在执行的ChangePeer operator的数量
    bvar::Adder<uint32_t> changeOpNum;
    // 正在执行的normal级别的operator的数量
    bvar::Adder<uint32_t> normalOpNum;
    // 正在执行的high级别的operator的数量
    bvar::Adder<uint32_t> highOpNum;
    // 正在执行的具体的operator
    std::map<CopySetKey, StringStatus> operators;

 private:
    std::shared_ptr<Topology> topo_;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULEMETRICS_H_


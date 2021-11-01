/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-11-8 11:01:48
 * @Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_SCHEDULE_SCHEDULEMETRICS_H_
#define CURVEFS_SRC_MDS_SCHEDULE_SCHEDULEMETRICS_H_

#include <bvar/bvar.h>
#include <map>
#include <memory>
#include <string>
#include "curvefs/src/mds/schedule/operatorController.h"
#include "src/common/stringstatus.h"

using ::curvefs::mds::heartbeat::ConfigChangeType;
using ::curve::common::StringStatus;

namespace curvefs {
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
    explicit ScheduleMetrics(std::shared_ptr<Topology> topo)
        : operatorNum(ScheduleMetricsPrefix, "operator_num"),
          addOpNum(ScheduleMetricsPrefix, "addPeer_num"),
          removeOpNum(ScheduleMetricsPrefix, "removePeer_num"),
          transferOpNum(ScheduleMetricsPrefix, "transferLeader_num"),
          changeOpNum(ScheduleMetricsPrefix, "changePeer_num"),
          normalOpNum(ScheduleMetricsPrefix, "normal_operator_num"),
          highOpNum(ScheduleMetricsPrefix, "high_operator_num"),
          topo_(topo) {}

    /**
     * @brief UpdateAddMetric Interface exposed to operatorContoller for
     *                        updating metric when adding operator
     *
     * @param[in] op Specific operator
     */
    void UpdateAddMetric(const Operator &op);

    /**
     * @brief UpdateMetric Interface exposed to operatorContoller for updating
     *                     metric when deleting operator
     *
     * @param[in] op Specific operator
     */
    void UpdateRemoveMetric(const Operator &op);

 private:
    /**
     * @brief GetHostNameAndPortById Get hostName:port of metaserver specified
     *                               by its ID
     *
     * @param[in] csid Metaserver ID
     *
     * @return hostName:port (a string)
     */
    std::string GetHostNameAndPortById(MetaServerIdType csid);

    /**
     * @brief GetOpPriorityStr Get the name of the priority level in string
     *
     * @param[in] pri Priority (OperatorPriority, which is an enum)
     *
     * @return corresponding level in string
     */
    std::string GetOpPriorityStr(OperatorPriority pri);

    /**
     * @brief RemoveUpdateOperatorsMap Update operator map when removing one of
     *                                 them
     *
     * @param[in] op Specified operator
     * @param[in] type Operator type, including
     * AddPeer/RemovePeer/TransferLeader //NOLINT
     * @param[in] target Target metaserver to change
     */
    void RemoveUpdateOperatorsMap(const Operator &op, std::string type,
                                  MetaServerIdType target);

    /**
     * @brief AddUpdateOperatorsMap Update operator map when adding a new one
     *
     * @param[in] op Specified operator
     * @param[in] type Operator type, including
     * AddPeer/RemovePeer/TransferLeader //NOLINT
     * @param[in] target Target metaserver to change
     */
    void AddUpdateOperatorsMap(const Operator &op, std::string type,
                               MetaServerIdType target);

    /**
     * @brief UpdateOperatorsMap Construct operator map for exporting and
     *                           transfer to JSON format
     *
     * @param[in] op Specified operator
     * @param[in] type Operator type, including
     * AddPeer/RemovePeer/TransferLeader //NOLINT
     * @param[in] target Target metaserver
     */
    void UpdateOperatorsMap(const Operator &op, std::string type,
                            MetaServerIdType target);

 public:
    const std::string ScheduleMetricsPrefix = "mds_scheduler_metric_";
    const std::string ScheduleMetricsCopySetOpPrefix =
        "mds_scheduler_metric_copyset_";

    // number of operator under execution
    bvar::Adder<uint32_t> operatorNum;
    // xxxNUM: number of operator xxx under execution
    bvar::Adder<uint32_t> addOpNum;
    bvar::Adder<uint32_t> removeOpNum;
    bvar::Adder<uint32_t> transferOpNum;
    bvar::Adder<uint32_t> changeOpNum;
    bvar::Adder<uint32_t> normalOpNum;
    bvar::Adder<uint32_t> highOpNum;
    // specific operator under execution
    std::map<CopySetKey, StringStatus> operators;

 private:
    std::shared_ptr<Topology> topo_;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SCHEDULE_SCHEDULEMETRICS_H_

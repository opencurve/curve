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
 * Created Date: Thu Nov 16 2018
 * Author: lixiaocui
 */

#ifndef SRC_MDS_SCHEDULE_COORDINATOR_H_
#define SRC_MDS_SCHEDULE_COORDINATOR_H_

#include <vector>
#include <map>
#include <memory>
#include <string>
#include <thread>  //NOLINT
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
     * @brief deal with copyset info reported by the chunkserver
     *
     * @param[in] originInfo Copyset info reported by heartbeat
     * @param[out] newConf   Configuration change generated for chunkserver
     *
     * @return candidate ID if there's any new configuration generated,
     *         UNINTIALIZE_ID if not
     */
    virtual ChunkServerIdType CopySetHeartbeat(
        const ::curve::mds::topology::CopySetInfo &originInfo,
        const ::curve::mds::heartbeat::ConfigChangeInfo &configChInfo,
        ::curve::mds::heartbeat::CopySetConf *newConf);
    /**
     * @brief deal with rapid leader balancing request
     *
     * @param[in] lpid The logical pool that require rapid leader balance
     *
     * @return kScheduleErrCodeSuccess If transferleader operators generated successfully //NOLINT
     *         kScheduleErrCodeInvalidLogicalPool If logical pool not exist
     */
    virtual int RapidLeaderSchedule(PoolIdType lpid);

    virtual int QueryChunkServerRecoverStatus(
        const std::vector<ChunkServerIdType> &idList,
        std::map<ChunkServerIdType, bool> *statusMap);

    /**
     * @brief determine whether the specified chunkserver is the target of the
     *        AddOperator of specified copyset
     *
     * @param[in] csId Chunkserver specified
     * @param[in] key Copyset specified
     */
    virtual bool ChunkserverGoingToAdd(ChunkServerIdType csId, CopySetKey key);

    /**
     * @brief Set specify logical pool to enable/disable scan
     * @param[in] lpid logical pool id
     * @param[in] scanEnable enable(true)/disable(false) scan
     * @return kScheduleErrCodeSuccess if set success,
     *         kScheduleErrCodeInvalidLogicalPool if logical pool not exist
     */
    virtual int SetLogicalPoolScanState(PoolIdType lpid, bool scanEnable);

    /**
     * @brief Initialize the scheduler according to the configuration
     *
     * @param[in] conf
     * @param[in] metrics ScheduleMetric for calculation when
     *                    adding/delecting operator
     */
    void InitScheduler(
        const ScheduleOption &conf, std::shared_ptr<ScheduleMetrics> metrics);

    /**
     * @brief run schedulers in background according to scheduler configuration
     */
    void Run();

    /**
     * @brief stop background scheduler threads
     */
    void Stop();

    // TODO(lixiaocui): external interface, and add according to the requirement
    //                  of operation and mantainance
    /**
     * @brief interface for the administrator
     *
     * @param[in] id CopysetID
     * @param[in] type Config change type: transfer-leader/add-peer/remove-peer
     * @param[in] item Item changed. It would be new leader ID when
     *                 transfer-leader, add target when add-peer, removed target
     *                 when remove a peer
     */
    void DoConfigChange(CopySetKey id,
                        ConfigChangeType type,
                        ChunkServerIdType item);

    /**
    * @brief For test unit
    */
    std::shared_ptr<OperatorController> GetOpController();

 private:
    /**
     * @brief SetScheduleRunning Stop all the schedulers if set to false
     *
     * @param[in] flag
     */
    void SetSchedulerRunning(bool flag);

    /**
     * @brief regular task for running different scheduler
     *
     * @param[in] s Schedulers for running
     * @param[in] type Scheduler type
     */
    void RunScheduler(const std::shared_ptr<Scheduler> &s, SchedulerType type);

    /**
     * @brief BuildCopySetConf Build copyset configuration for chunkserver
     *
     * @param[in] res Result of applyOperator
     * @param[out] copyset configuration for chunkserver
     *
     * @return true if succeeded and false if failed
     */
    bool BuildCopySetConf(
        const CopySetConf &res, ::curve::mds::heartbeat::CopySetConf *out);

    /**
     * @brief ScheduleNeedRun Determine whether specific type of scheduler is
     *                        allowed to run
     *
     * @param[in] type Scheduler type
     *
     * @return true if allow, false is not
     */
    bool ScheduleNeedRun(SchedulerType type);

    /**
     * @brief ScheduleName Name of specific type of scheduler
     *
     * @param[in] type Scheduler type
     *
     * @return scheduler name
     */
    std::string ScheduleName(SchedulerType type);

    bool IsChunkServerRecover(const ChunkServerInfo &info);

 private:
    std::shared_ptr<TopoAdapter> topo_;
    ScheduleOption conf_;

    std::map<SchedulerType, std::shared_ptr<Scheduler>> schedulerController_;
    std::map<SchedulerType, common::Thread> runSchedulerThreads_;
    std::shared_ptr<OperatorController> opController_;

    InterruptibleSleeper sleeper_;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_COORDINATOR_H_

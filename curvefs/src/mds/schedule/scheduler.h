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

#ifndef CURVEFS_SRC_MDS_SCHEDULE_SCHEDULER_H_
#define CURVEFS_SRC_MDS_SCHEDULE_SCHEDULER_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "curvefs/src/mds/schedule/operatorController.h"
#include "curvefs/src/mds/schedule/schedule_define.h"
#include "curvefs/src/mds/schedule/topoAdapter.h"
#include "curvefs/src/mds/topology/topology.h"

using ::curvefs::mds::topology::UNINITIALIZE_ID;
using ::curvefs::mds::topology::PoolIdType;
using ::curvefs::mds::topology::ZoneIdType;
using ::curvefs::mds::topology::ServerIdType;
using ::curvefs::mds::topology::MetaServerIdType;
using ::curvefs::mds::topology::CopySetIdType;

namespace curvefs {
namespace mds {
namespace schedule {

class Scheduler {
 public:
    /**
     * @brief Scheduler constructor
     *
     * @param[in] opt Options
     * @param[in] topo Topology info
     * @param[in] opController Operator management module
     */
    Scheduler(const ScheduleOption &opt,
              const std::shared_ptr<TopoAdapter> &topo,
              const std::shared_ptr<OperatorController> &opController)
        : topo_(topo), opController_(opController) {
        transTimeSec_ = opt.transferLeaderTimeLimitSec;
        removeTimeSec_ = opt.removePeerTimeLimitSec;
        changeTimeSec_ = opt.changePeerTimeLimitSec;
        addTimeSec_ = opt.addPeerTimeLimitSec;
    }

    /**
     * @brief producing operator according to cluster status
     */
    virtual int Schedule();

    /**
     * @brief time interval of generating operations
     */
    virtual int64_t GetRunningInterval();

 public:
    /**
     * @brief SelectBestPlacementMetaServer Select a healthy metaserver in
     *                                       the cluster to replace the oldPeer
     *                                       in copysetInfo
     *
     * @param[in] copySetInfo
     * @param[in] copySet Replica to replace
     *
     * @return target metaserver, return UNINITIALIZED if no
     *         metaserver selected
     */
    MetaServerIdType SelectBestPlacementMetaServer(
        const CopySetInfo &copySetInfo, MetaServerIdType oldPeer);

 protected:
    std::shared_ptr<TopoAdapter> topo_;
    // operator management module
    std::shared_ptr<OperatorController> opController_;

    // maximum estimated time of transferring the leader, alarm if exceeded
    int transTimeSec_;
    // maximum estimated tim for adding peers, alarm if exceeded
    int addTimeSec_;
    // maximum estimated time for removing peer, alarm if exceeded
    int removeTimeSec_;
    // maximum estimated time for changing peer, alarm if exceeded
    int changeTimeSec_;
};

// recovering the offline replicas
class RecoverScheduler : public Scheduler {
 public:
    RecoverScheduler(const ScheduleOption &opt,
                     const std::shared_ptr<TopoAdapter> &topo,
                     const std::shared_ptr<OperatorController> &opController)
        : Scheduler(opt, topo, opController) {
        runInterval_ = opt.recoverSchedulerIntervalSec;
    }

    /**
     * @brief recovering the offline replica on Topology
     *
     * @return the number of operators generated
     */
    int Schedule() override;

    /**
     * @brief running time interval of the scheduler
     *
     * @return time interval
     */
    int64_t GetRunningInterval() override;

 private:
    /**
     * @brief fix the specified replica
     *
     * @param[in] info The copyset to be fixed
     * @param[in] peerId The metaserver(replica) to be fixed
     * @param[out] op The operator generated
     * @param[out] target The replica added
     *
     * @return Whether any operator has been generated
     */
    bool FixOfflinePeer(const CopySetInfo &info, MetaServerIdType peerId,
                        Operator *op, MetaServerIdType *target);

 private:
    // running interval of RecoverScheduler
    int64_t runInterval_;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SCHEDULE_SCHEDULER_H_

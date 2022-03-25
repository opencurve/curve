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

#ifndef CURVEFS_SRC_MDS_SCHEDULE_SCHEDULE_DEFINE_H_
#define CURVEFS_SRC_MDS_SCHEDULE_SCHEDULE_DEFINE_H_

namespace curvefs {
namespace mds {
namespace schedule {

// only support one type now, add more type later
enum class SchedulerType {
    RecoverSchedulerType,
    CopysetSchedulerType,
};

struct ScheduleOption {
 public:
    // recover switch
    bool enableRecoverScheduler;
    // copyset scheduler switch
    bool enableCopysetScheduler;

    // xxxSchedulerIntervalSec: time interval of calculation for xxx scheduling
    uint32_t recoverSchedulerIntervalSec;
    uint32_t copysetSchedulerIntervalSec;

    // number of copyset that can operate configuration changing at the same time on single metaserver //NOLINT
    uint32_t operatorConcurrent;

    // xxxTimeLimitSec: time limit for xxx, operation will be considered
    // overtime and cancel if exceed this limit
    uint32_t transferLeaderTimeLimitSec;
    uint32_t addPeerTimeLimitSec;
    uint32_t removePeerTimeLimitSec;
    uint32_t changePeerTimeLimitSec;

    // metaserver can be the target leader on leader scheduling only after
    // starting for metaserverCoolingTimeSec.
    // when a metaserver start running, the copysets will replay the journal,
    // and during the leader transferring I/O on metaserver will be suspended.
    // if the metaserver is under journal replaying when leader transferring
    // operation arrive, the operation will wait for the replay and will be
    // stuck and exceed the 'leadertimeout' if the replay takes too long time.
    uint32_t metaserverCoolingTimeSec;

    uint32_t balanceRatioPercent;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SCHEDULE_SCHEDULE_DEFINE_H_

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
    bool enableCopysetScheduler;
    bool enableLeaderScheduler;
    bool enableRecoverScheduler;
    bool enableReplicaScheduler;

    // xxxSchedulerIntervalSec: time interval of calculation for xxx scheduling
    uint32_t copysetSchedulerIntervalSec;
    uint32_t leaderSchedulerIntervalSec;
    uint32_t recoverSchedulerIntervalSec;
    uint32_t replicaSchedulerIntervalSec;

    // number of copyset that can operate configuration changing at the same time on single chunkserver //NOLINT
    uint32_t operatorConcurrent;

    // xxxTimeLimitSec: time limit for xxx, operation will be considered
    // overtime and cancel if exceed this limit
    uint32_t transferLeaderTimeLimitSec;
    uint32_t addPeerTimeLimitSec;
    uint32_t removePeerTimeLimitSec;
    uint32_t changePeerTimeLimitSec;

    // for copysetScheduler, the (range of the number of copyset on chunkserver)
    // should not exceed (average number of copyset on chunkserver * copysetNumRangePercent) //NOLINT
    float copysetNumRangePercent;
    // configuration changes should try to guarantee that the scatter-width
    // of chunkserver not exceed minScatterWith * (1 + scatterWidthRangePerent)
    float scatterWithRangePerent;
    // the failing chunkserver threshold for operating a recovery, if exceed, no
    // attempt on recovery will be committed
    uint32_t chunkserverFailureTolerance;
    // chunkserver can be the target leader on leader scheduling only after
    // starting for chunkserverCoolingTimeSec.
    // when a chunkserver start running, the copysets will replay the journal,
    // and during the leader transferring I/O on chunkserver will be suspended.
    // if the chunkserver is under journal replaying when leader transferring
    // operation arrive, the operation will wait for the replay and will be
    // stuck and exceed the 'leadertimeout' if the replay takes too long time.
    uint32_t chunkserverCoolingTimeSec;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_SCHEDULE_DEFINE_H_

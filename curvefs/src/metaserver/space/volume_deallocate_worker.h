/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Date: Tue Apr 25 16:13:06 CST 2023
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_METASERVER_SPACE_VOLUME_DEALLOCATE_WORKER_H_
#define CURVEFS_SRC_METASERVER_SPACE_VOLUME_DEALLOCATE_WORKER_H_

#include <condition_variable>
#include <thread>
#include <list>
#include <unordered_map>

#include "absl/types/optional.h"

#include "src/common/interruptible_sleeper.h"
#include "curvefs/src/metaserver/space/inode_volume_space_deallocate.h"

namespace curvefs {
namespace metaserver {

class VolumeDeallocateWorker;

struct VolumeDeallocateWorkerContext {
    std::atomic<bool> running{false};

    std::mutex mtx;
    std::condition_variable cond;
    std::list<InodeVolumeSpaceDeallocate> allTasks;
    std::unordered_map<uint32_t, VolumeDeallocateWorker *> tasking;
};

class VolumeDeallocateWorker {
 public:
    explicit VolumeDeallocateWorker(VolumeDeallocateWorkerContext *context)
        : ctx_(context) {}

    void Run();
    void Stop();
    void Cancel(uint32_t partitionId);
    void SetDeallocate(uint64_t blockGroupOffset);
    bool HasDeallocate();

 private:
    bool WaitDeallocate();
    void Cleanup();
    void DeallocatetWorker();
    void ExecuteDeallocateWorker();

 private:
    VolumeDeallocateWorkerContext *ctx_;
    std::thread calWorkingThread_;

    // current cal
    absl::optional<InodeVolumeSpaceDeallocate> current_;

    curve::common::InterruptibleSleeper sleeper_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_SPACE_VOLUME_DEALLOCATE_WORKER_H_

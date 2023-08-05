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
 * Date: Tue Apr 25 15:36:29 CST 2023
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_METASERVER_SPACE_VOLUME_DEALLOCATE_MANAGER_H_
#define CURVEFS_SRC_METASERVER_SPACE_VOLUME_DEALLOCATE_MANAGER_H_

#include <vector>
#include <memory>

#include "curvefs/src/metaserver/space/volume_deallocate_worker.h"
#include "curvefs/src/metaserver/space/inode_volume_space_deallocate.h"

namespace curvefs {
namespace metaserver {

struct VolumeDeallocateWorkerQueueOption {
    bool enable = false;
    uint32_t workerNum = 1;
};

class VolumeDeallocateManager {
 public:
    static VolumeDeallocateManager &GetInstance() {
        static VolumeDeallocateManager instance;
        return instance;
    }

    void Init(const VolumeDeallocateWorkerQueueOption &option,
              const VolumeDeallocateExecuteOption &executeOpt) {
        workerOpt_.enable = option.enable;
        workerOpt_.workerNum = option.workerNum;

        executeOpt_.volumeSpaceManager = executeOpt.volumeSpaceManager;
        executeOpt_.metaClient = executeOpt.metaClient;
        executeOpt_.batchClean = executeOpt.batchClean;
    }

    void Register(InodeVolumeSpaceDeallocate task);
    void Cancel(uint32_t partitionId);
    void Deallocate(uint32_t partitioId, uint64_t blockGroupOffset);
    bool HasDeallocate();

    void Run();
    void Stop();



 private:
    VolumeDeallocateManager() = default;
    ~VolumeDeallocateManager() = default;

    VolumeDeallocateWorkerQueueOption workerOpt_;
    VolumeDeallocateExecuteOption executeOpt_;

    VolumeDeallocateWorkerContext workerCtx_;
    std::vector<std::unique_ptr<VolumeDeallocateWorker>> workers_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_SPACE_VOLUME_DEALLOCATE_MANAGER_H_

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

#ifndef CURVEFS_SRC_CLIENT_FSUSED_UPDATER_H_
#define CURVEFS_SRC_CLIENT_FSUSED_UPDATER_H_

#include <time.h>
#include <brpc/periodic_task.h>
#include "curvefs/src/client/rpcclient/metaserver_client.h"

namespace curvefs {
namespace client {

class FsUsedUpdater {
 public:
    static FsUsedUpdater& GetInstance() {
        static FsUsedUpdater instance_;
        return instance_;
    }

    void Init(uint32_t fsId,
              std::shared_ptr<rpcclient::MetaServerClient> metaserverClient) {
        fsId_ = fsId;
        metaserverClient_ = metaserverClient;
        deltaBytes_.store(0);
    }

    void UpdateDeltaBytes(int64_t deltaBytes);

    void UpdateFsUsed();

    int64_t GetDeltaBytes();

 private:
    uint32_t fsId_;
    std::atomic<int64_t> deltaBytes_;
    std::shared_ptr<rpcclient::MetaServerClient> metaserverClient_;
};

class UpdateFsUsedTask : public brpc::PeriodicTask {
 public:
    explicit UpdateFsUsedTask(FsUsedUpdater* fsUsedUpdater, int64_t interval_s)
        : fsUsedUpdater_(fsUsedUpdater), interval_s_(interval_s) {}
    bool OnTriggeringTask(timespec* next_abstime) override;
    void OnDestroyingTask() override;

 private:
    FsUsedUpdater* fsUsedUpdater_;
    int64_t interval_s_;
};

void StartUpdateFsUsedTask(FsUsedUpdater* updater, int64_t interval_s);

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FSUSED_UPDATER_H_

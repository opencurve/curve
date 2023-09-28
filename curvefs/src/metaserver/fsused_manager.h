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

#ifndef CURVEFS_SRC_METASERVER_FSUSED_MANAGER_H_
#define CURVEFS_SRC_METASERVER_FSUSED_MANAGER_H_

#include <time.h>
#include <brpc/periodic_task.h>
#include <bthread/mutex.h>
#include <deque>
#include <memory>
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/proto/metaserver.pb.h"

namespace curvefs {
namespace metaserver {

using curvefs::client::rpcclient::MetaServerClient;

// fs level used manager
class FsUsedManager {
 public:
    explicit FsUsedManager(std::shared_ptr<MetaServerClient> metaserverClient)
        : metaserverClient_(metaserverClient), fsUsedDeltas_() {}

    void AddFsUsedDelta(FsUsedDelta&& fsUsedDelta);
    void ApplyFsUsedDeltas();

 private:
    std::shared_ptr<MetaServerClient> metaserverClient_;
    std::deque<FsUsedDelta> fsUsedDeltas_;
    bthread::Mutex dirtyLock_;
};

class UpdateFsUsedTask : public brpc::PeriodicTask {
 public:
    explicit UpdateFsUsedTask(FsUsedManager* fsUsedManager, int64_t interval_s)
        : fsUsedManager_(fsUsedManager), interval_s_(interval_s) {}
    bool OnTriggeringTask(timespec* next_abstime) override;
    void OnDestroyingTask() override;

 private:
    FsUsedManager* fsUsedManager_;
    int64_t interval_s_;
};

void StartUpdateFsUsedTask(FsUsedManager* fsUsedManager, int64_t interval_s);

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_FSUSED_MANAGER_H_

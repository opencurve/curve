/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Tuesday Mar 01 12:40:43 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/mds/space/reloader.h"

#include <butil/time.h>
#include <glog/logging.h>

#include "absl/memory/memory.h"
#include "curvefs/proto/mds.pb.h"

namespace curvefs {
namespace mds {
namespace space {

using ::curve::common::TaskThreadPool;

Reloader::Reloader(SpaceManager* manager, int concurrency)
    : manager_(manager),
      taskPool_(absl::make_unique<TaskThreadPool<>>()),
      error_(SpaceOk) {
    taskPool_->Start(concurrency);
}

void Reloader::Add(const FsInfo& fsInfo) {
    taskPool_->Enqueue(&Reloader::ReloadOne, this, fsInfo);
}

void Reloader::ReloadOne(const FsInfo& fsInfo) {
    butil::Timer timer;
    timer.start();
    auto err = manager_->AddVolume(fsInfo);
    timer.stop();

    if (err != SpaceOk) {
        LOG(WARNING) << "Reload volume space failed, fsId: " << fsInfo.fsid()
                     << ", err: " << SpaceErrCode_Name(err);

        // only record the first occurred error
        SpaceErrCode expected = SpaceOk;
        error_.compare_exchange_strong(expected, err,
                                       std::memory_order_relaxed);
    } else {
        LOG(INFO) << "Reload volume space succeeded, fsId: " << fsInfo.fsid()
                  << ", elapsed " << timer.m_elapsed(.0) << " ms";
    }
}

SpaceErrCode Reloader::Wait() {
    while (taskPool_->QueueSize() != 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return error_;
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs

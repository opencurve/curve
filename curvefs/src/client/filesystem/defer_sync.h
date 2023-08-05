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
 * Project: Curve
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_DEFER_SYNC_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_DEFER_SYNC_H_

#include <atomic>
#include <vector>
#include <memory>

#include "src/common/interruptible_sleeper.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/filesystem/meta.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curvefs::client::common::DeferSyncOption;

using ::curve::common::Mutex;
using ::curve::common::LockGuard;
using ::curve::common::InterruptibleSleeper;

class DeferSync {
 public:
    explicit DeferSync(DeferSyncOption option);

    void Start();

    void Stop();

    void Push(const std::shared_ptr<InodeWrapper>& inode);

 private:
    void SyncTask();

 private:
    DeferSyncOption option_;
    Mutex mutex_;
    std::atomic<bool> running_;
    std::thread thread_;
    InterruptibleSleeper sleeper_;
    std::vector<std::shared_ptr<InodeWrapper>> inodes_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_DEFER_SYNC_H_

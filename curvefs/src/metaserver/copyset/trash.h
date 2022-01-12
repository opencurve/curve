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
 * Project: curve
 * Created Date: Sun 29 Aug 2021 03:29:15 PM CST
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_TRASH_H_
#define CURVEFS_SRC_METASERVER_COPYSET_TRASH_H_

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

#include "src/common/interruptible_sleeper.h"
#include "src/fs/local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

struct CopysetTrashOptions {
    // trash path
    // format is ${protocol}://{relative or absolute path}
    // e.g., local:///mnt/trash
    std::string trashUri;

    // after a copyset has been moved to trashUri for |expiredAfterSec| seconds
    // its data can be deleted
    // Default: 1 hour
    uint32_t expiredAfterSec;

    // backend thread scan interval in seconds
    // Default: 5 minnute
    uint32_t scanPeriodSec;

    CopysetTrashOptions()
        : trashUri(), expiredAfterSec(60 * 60), scanPeriodSec(5 * 60) {}
};

// when heartbeat judges that current server is not int the coyset
// configuration group, it notifies copyset node manager to move the copyset
// data directory to the recycle bin directory, and reclaim the physical
// space after a period time
class CopysetTrash {
 public:
    CopysetTrash();

    bool Init(const CopysetTrashOptions& options,
              curve::fs::LocalFileSystem* fs);

    bool Start();

    bool Stop();

    bool RecycleCopyset(const std::string& copysetAbsolutePath);

 private:
    void DeleteExpiredCopysets();

    bool CreateTrashDirIfNotExist();

    std::string GenerateCopysetRecyclePath(
        const std::string& copysetAbsolutePath);

    bool IsCopysetDirExpired(const std::string& dir);

    bool IsCopysetDirNameValid(const std::string& dir) const;

 private:
    CopysetTrashOptions options_;
    curve::fs::LocalFileSystem* lfs_;
    std::string trashDir_;
    std::atomic<bool> running_;
    std::thread recycleThread_;
    curve::common::InterruptibleSleeper sleeper_;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_TRASH_H_

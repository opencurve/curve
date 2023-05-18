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
 * Date: Mon Aug  9 16:27:00 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_CONFIG_H_
#define CURVEFS_SRC_METASERVER_COPYSET_CONFIG_H_

#include <braft/raft.h>
#include <braft/snapshot_throttle.h>
#include <gflags/gflags.h>

#include <cstdint>
#include <memory>
#include <string>

#include "curvefs/src/metaserver/copyset/concurrent_apply_queue.h"
#include "curvefs/src/metaserver/copyset/trash.h"
#include "curvefs/src/metaserver/storage/config.h"
#include "src/fs/local_filesystem.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

// Options for copyset node and relative modules
struct CopysetNodeOptions {
    // copyset's data uri
    std::string dataUri;

    // ip and port of this copyset node
    std::string ip;
    uint32_t port;

    // the number of concurrent recovery loads of copyset
    // Default: 1
    uint32_t loadConcurrency;

    // the maximum number of retries to check whether a copyset is loaded
    // completed, possible exceptions:
    // 1. most of the current replicas haven't been up
    // 2. network problems, etc. lead to the failure to obtain the leader
    // 3. the committed index of the leader cannot be obtained due to other
    //    reasons
    // Default: 3
    uint32_t checkRetryTimes;

    // if the difference between the applied_index of the current peer and the
    // committed_index on the leader is less than |finishLoadMargin|, it's
    // determined that the copyset has been loaded
    // Default: 2000
    uint32_t finishLoadMargin;

    // sleep time in microseconds between different cycles check whether
    // copyset is loaded
    // Default: 1000
    uint32_t checkLoadMarginIntervalMs;

    // apply queue options
    ApplyOption applyQueueOption;

    // filesystem adaptor
    curve::fs::LocalFileSystem* localFileSystem;

    CopysetTrashOptions trashOptions;

    braft::NodeOptions raftNodeOptions;

    storage::StorageOptions storageOptions;

    CopysetNodeOptions();
};

inline CopysetNodeOptions::CopysetNodeOptions()
    : dataUri(),
      ip(),
      port(-1),
      loadConcurrency(1),
      checkRetryTimes(3),
      finishLoadMargin(2000),
      checkLoadMarginIntervalMs(1000),
      applyQueueOption(),
      localFileSystem(nullptr),
      trashOptions(),
      raftNodeOptions() {}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_CONFIG_H_

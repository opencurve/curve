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
 * Date: Wednesday Jul 13 14:16:48 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_S3COMPACT_WORKER_H_
#define CURVEFS_SRC_METASERVER_S3COMPACT_WORKER_H_

#include <atomic>
#include <condition_variable>
#include <list>
#include <map>
#include <mutex>
#include <thread>

#include "absl/types/optional.h"
#include "curvefs/src/metaserver/s3compact.h"
#include "src/common/interruptible_sleeper.h"

namespace curvefs {
namespace metaserver {

namespace copyset {
class CopysetNode;
}  // namespace copyset

class S3AdapterManager;
class S3CompactManager;
class S3CompactWorker;
class S3InfoCache;

struct S3CompactWorkerContext {
    std::atomic<bool> running{false};

    std::mutex mtx;
    std::condition_variable cond;
    std::list<S3Compact> s3compacts;

    // compacting partitions
    std::map<uint32_t, S3CompactWorker*> compacting;
};

struct S3CompactWorkerOptions {
    S3AdapterManager* s3adapterManager;
    S3InfoCache* s3infoCache;

    uint64_t maxChunksPerCompact;
    uint64_t fragmentThreshold;
    uint64_t s3ReadMaxRetry;
    uint64_t s3ReadRetryInterval;

    // sleep interval in ms between compacting two inodes
    uint64_t sleepMS;
};

// S3CompactWorker compacts one partition at once
class S3CompactWorker {
 public:
    S3CompactWorker(S3CompactManager* manager,
                    S3CompactWorkerContext* context,
                    S3CompactWorkerOptions* options);

    void Run();

    void Stop();

    // Cancel current compaction job
    void Cancel(uint32_t partitionId);

 private:
    // Worker function
    void CompactWorker();

    // Return true if we've got a partition to compact, otherwise return false
    bool WaitCompact();

    // Return whether compact current partition again
    bool CompactInodes(const std::list<uint64_t>& inodes,
                       copyset::CopysetNode* node);

    void CleanupCompact(bool again);

 private:
    S3CompactManager* manager_;
    S3CompactWorkerContext* context_;
    S3CompactWorkerOptions* options_;

    std::thread compact_;

    // current compaction info, if in waiting state, it doesn't has value
    absl::optional<S3Compact> s3Compact_;

    curve::common::InterruptibleSleeper sleeper;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_S3COMPACT_WORKER_H_

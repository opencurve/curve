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
 * Date: Wednesday Jul 13 15:58:59 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/s3compact_worker.h"

#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/common/threading.h"
#include "curvefs/src/metaserver/s3compact.h"
#include "curvefs/src/metaserver/s3compact_inode.h"
#include "curvefs/src/metaserver/storage/converter.h"

namespace curvefs {
namespace metaserver {

S3CompactWorker::S3CompactWorker(S3CompactManager* manager,
                                 S3CompactWorkerContext* context,
                                 S3CompactWorkerOptions* options)
    : manager_(manager),
      context_(context),
      options_(options) {}

// REQUIRES: context_->mtx is held
void S3CompactWorker::Cancel(uint32_t partitionId) {
    assert(s3Compact_->partitionInfo.partitionid() == partitionId);
    (void)partitionId;
    s3Compact_->canceled = true;
    sleeper.interrupt();
}

void S3CompactWorker::Run() {
    compact_ = std::thread{&S3CompactWorker::CompactWorker, this};
}

void S3CompactWorker::Stop() {
    sleeper.interrupt();

    if (compact_.joinable()) {
        compact_.join();
    }

    LOG(INFO) << "S3CompactWorker stopped";
}

bool S3CompactWorker::WaitCompact() {
    std::unique_lock<std::mutex> lock(context_->mtx);
    if (context_->s3compacts.empty()) {
        context_->cond.wait(lock, [this]() {
            return !context_->s3compacts.empty() || !context_->running;
        });
    }

    if (!context_->running) {
        return false;
    }

    s3Compact_ = std::move(context_->s3compacts.front());
    context_->s3compacts.pop_front();
    context_->compacting.emplace(s3Compact_->partitionInfo.partitionid(), this);

    sleeper.init();

    return true;
}

void S3CompactWorker::CleanupCompact(bool again) {
    std::lock_guard<std::mutex> lock(context_->mtx);

    context_->compacting.erase(s3Compact_->partitionInfo.partitionid());

    if (s3Compact_->canceled) {
        LOG(INFO) << "Compact for partition "
                  << s3Compact_->partitionInfo.partitionid()
                  << " is asked to cancel";
        again = false;
    }

    if (again) {
        context_->s3compacts.push_back(std::move(s3Compact_.value()));
        context_->cond.notify_one();
    }

    s3Compact_.reset();
}

bool S3CompactWorker::CompactInodes(const std::list<uint64_t>& inodes,
                                    copyset::CopysetNode* node) {
    if (inodes.empty()) {
        VLOG(1) << "inode list is empty";
        sleeper.wait_for(std::chrono::milliseconds(options_->sleepMS));
        return true;
    }

    const auto fsId = s3Compact_->partitionInfo.fsid();
    const auto pid = s3Compact_->partitionInfo.partitionid();

    for (auto ino : inodes) {
        if (!sleeper.wait_for(std::chrono::milliseconds(options_->sleepMS))) {
            return false;
        }

        if (!context_->running) {
            return false;
        }

        if (s3Compact_->canceled) {
            LOG(INFO) << "Compact for partition " << pid
                      << " is asked to cancel";
            return false;
        }

        if (!node->IsLeaderTerm()) {
            VLOG(1)
                << "Current copyset " << node->Name()
                << " isn't leader, skip this around compaction for partition `"
                << pid << "`";
            return true;
        }

        CompactInodeJob::S3CompactTask task{
            s3Compact_->inodeManager, storage::Key4Inode{fsId, ino},
            s3Compact_->partitionInfo,
            absl::make_unique<CopysetNodeWrapper>(node)
        };

        CompactInodeJob job(options_);
        job.CompactChunks(task);
    }

    return true;
}

void S3CompactWorker::CompactWorker() {
    common::SetThreadName("s3compact");

    while (context_->running) {
        if (!WaitCompact()) {
            break;
        }

        CHECK(s3Compact_.has_value());
        VLOG(1) << "Going to compact: "
                << s3Compact_->partitionInfo.ShortDebugString();

        // we take the compaction job, so at the end we put it back if needed
        bool compactAgain = true;

        auto cleanup = absl::MakeCleanup([&, this]() {
            CleanupCompact(compactAgain);
        });

        const auto pid = s3Compact_->partitionInfo.partitionid();
        std::list<uint64_t> inodes;
        if (!s3Compact_->inodeManager->GetInodeIdList(&inodes)) {
            compactAgain = false;
            LOG(WARNING) << "Fail to GetInodeIdList for compact, partitionid: "
                         << pid << ", stop compact for this partition";
            continue;
        }

        if (s3Compact_->copysetNode == nullptr) {
            compactAgain = false;
            LOG(WARNING) << "Copyset node is invalid, poolid: "
                         << s3Compact_->partitionInfo.poolid()
                         << ", copysetid: "
                         << s3Compact_->partitionInfo.copysetid()
                         << ", partition id: " << pid
                         << ", stop compact for this partition";
            continue;
        }

        compactAgain = CompactInodes(inodes, s3Compact_->copysetNode.get());
    }

    LOG(INFO) << "S3CompactWorker thread stopped";
}

}  // namespace metaserver
}  // namespace curvefs

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
 * @Project: curve
 * @Date: 2021-09-07
 * @Author: majie1
 */

#include "curvefs/src/metaserver/s3compact_manager.h"

#include <thread>

#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "curvefs/src/metaserver/s3compact_wq_impl.h"

using curve::common::Configuration;
using curve::common::InitS3AdaptorOption;
using curve::common::ReadLockGuard;
using curve::common::S3Adapter;
using curve::common::S3AdapterOption;
using curve::common::TaskThreadPool;
using curve::common::WriteLockGuard;

namespace curvefs {
namespace metaserver {

using copyset::CopysetNodeManager;

void S3CompactWorkQueueOption::Init(std::shared_ptr<Configuration> conf) {
    InitS3AdaptorOption(conf.get(), &s3opts);
    conf->GetValueFatalIfFail("s3compactwq.enable", &enable);
    conf->GetValueFatalIfFail("s3compactwq.thread_num", &threadNum);
    conf->GetValueFatalIfFail("s3compactwq.queue_size", &queueSize);
    conf->GetValueFatalIfFail("s3compactwq.fragment_threshold",
                              &fragmentThreshold);
    conf->GetValueFatalIfFail("s3compactwq.max_chunks_per_compact",
                              &maxChunksPerCompact);
    conf->GetValueFatalIfFail("s3compactwq.enqueue_sleep_ms", &enqueueSleepMS);
    conf->GetValueFatalIfFail("s3.blocksize", &blockSize);
    conf->GetValueFatalIfFail("s3.chunksize", &chunkSize);
}

void S3CompactManager::Init(std::shared_ptr<Configuration> conf) {
    opts_.Init(conf);
    if (opts_.enable) {
        LOG(INFO) << "S3Compact is not enabled.";
        // start a s3 client
        s3Adapter_ = std::make_shared<S3Adapter>();
        s3Adapter_->Init(opts_.s3opts);
        s3compactworkqueueImpl_ =
            std::make_shared<S3CompactWorkQueueImpl>(s3Adapter_, opts_);
        inited_ = true;
    }
}

void S3CompactManager::RegisterS3Compact(std::shared_ptr<S3Compact> s3compact) {
    WriteLockGuard l(rwLock_);
    s3compacts_.emplace_back(s3compact);
}

int S3CompactManager::Run() {
    if (!inited_) {
        LOG(WARNING) << "S3Compact is not inited";
        return 0;
    }
    int ret = s3compactworkqueueImpl_->Start(opts_.threadNum, opts_.queueSize);
    if (ret != 0) return ret;
    entry_ = std::thread([this]() {
        while (inited_) {
            this->Enqueue();
        }
    });
    return 0;
}

void S3CompactManager::Stop() {
    if (inited_) {
        inited_ = false;
        s3compactworkqueueImpl_->Stop();
        entry_.join();
        s3Adapter_->Deinit();
    }
}

void S3CompactManager::Enqueue() {
    std::vector<std::shared_ptr<S3Compact>> s3compacts;
    {
        // make a snapshot
        ReadLockGuard l(rwLock_);
        s3compacts = s3compacts_;
    }
    if (s3compacts.empty()) {
        sleeper_.wait_for(std::chrono::milliseconds(opts_.enqueueSleepMS));
    }
    for (const auto& s3compact : s3compacts) {
        auto pinfo = s3compact->GetPartition();
        auto copysetNode = CopysetNodeManager::GetInstance().GetCopysetNode(
            pinfo.poolid(), pinfo.copysetid());
        // traverse inode container
        auto inodeStorage = s3compact->GetMutableInodeStorage();
        InodeStorage::ContainerType inodes(*(inodeStorage->GetContainer()));
        if (inodes.empty()) {
            sleeper_.wait_for(std::chrono::milliseconds(opts_.enqueueSleepMS));
            continue;
        }
        for (const auto& item : inodes) {
            sleeper_.wait_for(std::chrono::milliseconds(opts_.enqueueSleepMS));
            s3compactworkqueueImpl_->Enqueue(
                inodeStorage, InodeKey(item.second), pinfo, copysetNode);
        }
    }
}

}  // namespace metaserver
}  // namespace curvefs

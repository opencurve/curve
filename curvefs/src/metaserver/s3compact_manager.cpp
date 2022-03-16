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

#include <list>
#include <thread>

#include "src/common/string_util.h"
#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "curvefs/src/metaserver/s3compact_wq_impl.h"

using curve::common::Configuration;
using curve::common::InitS3AdaptorOptionExceptS3InfoOption;
using curve::common::ReadLockGuard;
using curve::common::S3Adapter;
using curve::common::S3AdapterOption;
using curve::common::TaskThreadPool;
using curve::common::WriteLockGuard;

namespace curvefs {
namespace metaserver {

using copyset::CopysetNodeManager;

void S3AdapterManager::Init() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (inited_) return;
    used_.resize(size_);
    for (int i = 0; i < size_; i++) {
        s3adapters_.emplace_back(new S3Adapter());
    }
    for (auto& s3adapter : s3adapters_) {
        s3adapter->Init(opts_);
    }
    inited_ = true;
}

void S3AdapterManager::Deinit() {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (inited_)
            inited_ = false;
        else
            return;
    }
    for (auto& s3adapter : s3adapters_) {
        s3adapter->Deinit();
    }
}

std::pair<uint64_t, S3Adapter*> S3AdapterManager::GetS3Adapter() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (!inited_) return std::make_pair(size_, nullptr);
    uint64_t i = 0;
    for (; i < size_; i++) {
        if (!used_[i]) {
            used_[i] = true;
            return std::make_pair(i, s3adapters_[i].get());
        }
    }
    return std::make_pair(size_, nullptr);
}

void S3AdapterManager::ReleaseS3Adapter(uint64_t index) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (!inited_) return;
    assert(index < used_.size());
    assert(used_[index] == true);
    used_[index] = false;
}

S3AdapterOption S3AdapterManager::GetBasicS3AdapterOption() {
    return opts_;
}

void S3CompactWorkQueueOption::Init(std::shared_ptr<Configuration> conf) {
    std::string mdsAddrsStr;
    conf->GetValueFatalIfFail("mds.listen.addr", &mdsAddrsStr);
    curve::common::SplitString(mdsAddrsStr, ",", &mdsAddrs);
    conf->GetValueFatalIfFail("global.ip", &metaserverIpStr);
    conf->GetValueFatalIfFail("global.port", &metaserverPort);
    // leave ak,sk,addr,bucket,chunksize,blocksize blank
    s3opts.ak = "";
    s3opts.sk = "";
    s3opts.s3Address = "";
    s3opts.bucketName = "";
    conf->GetValueFatalIfFail("s3.loglevel", &s3opts.loglevel);
    conf->GetStringValue("s3.logPrefix", &s3opts.logPrefix);
    conf->GetValueFatalIfFail("s3.http_scheme", &s3opts.scheme);
    conf->GetValueFatalIfFail("s3.verify_SSL", &s3opts.verifySsl);
    conf->GetValueFatalIfFail("s3.max_connections", &s3opts.maxConnections);
    conf->GetValueFatalIfFail("s3.connect_timeout", &s3opts.connectTimeout);
    conf->GetValueFatalIfFail("s3.request_timeout", &s3opts.requestTimeout);
    conf->GetValueFatalIfFail("s3.async_thread_num", &s3opts.asyncThreadNum);
    conf->GetValueFatalIfFail("s3.throttle.iopsTotalLimit",
                              &s3opts.iopsTotalLimit);
    conf->GetValueFatalIfFail("s3.throttle.iopsReadLimit",
                              &s3opts.iopsReadLimit);
    conf->GetValueFatalIfFail("s3.throttle.iopsWriteLimit",
                              &s3opts.iopsWriteLimit);
    conf->GetValueFatalIfFail("s3.throttle.bpsTotalMB", &s3opts.bpsTotalMB);
    conf->GetValueFatalIfFail("s3.throttle.bpsReadMB", &s3opts.bpsReadMB);
    conf->GetValueFatalIfFail("s3.throttle.bpsWriteMB", &s3opts.bpsWriteMB);
    conf->GetValueFatalIfFail("s3compactwq.enable", &enable);
    conf->GetValueFatalIfFail("s3compactwq.thread_num", &threadNum);
    conf->GetValueFatalIfFail("s3compactwq.queue_size", &queueSize);
    conf->GetValueFatalIfFail("s3compactwq.fragment_threshold",
                              &fragmentThreshold);
    conf->GetValueFatalIfFail("s3compactwq.max_chunks_per_compact",
                              &maxChunksPerCompact);
    conf->GetValueFatalIfFail("s3compactwq.enqueue_sleep_ms", &enqueueSleepMS);
    conf->GetValueFatalIfFail("s3compactwq.s3infocache_size", &s3infocacheSize);
    conf->GetValueFatalIfFail("s3compactwq.s3_read_max_retry", &s3ReadMaxRetry);
    conf->GetValueFatalIfFail("s3compactwq.s3_read_retry_interval",
                              &s3ReadRetryInterval);
}

void S3CompactManager::Init(std::shared_ptr<Configuration> conf) {
    opts_.Init(conf);
    if (opts_.enable) {
        LOG(INFO) << "s3compact: enabled.";
        butil::ip_t metaserverIp;
        if (butil::str2ip(opts_.metaserverIpStr.c_str(), &metaserverIp) < 0) {
            LOG(FATAL) << "Invalid Metaserver IP provided: "
                       << opts_.metaserverIpStr;
        }
        butil::EndPoint metaserverAddr_(metaserverIp, opts_.metaserverPort);
        LOG(INFO) << "Metaserver address: " << opts_.metaserverIpStr << ":"
                  << opts_.metaserverPort;
        s3infoCache_ = std::make_shared<S3InfoCache>(
            opts_.s3infocacheSize, opts_.mdsAddrs, metaserverAddr_);
        s3adapterManager_ =
            std::make_shared<S3AdapterManager>(opts_.queueSize, opts_.s3opts);
        s3adapterManager_->Init();
        s3compactworkqueueImpl_ = std::make_shared<S3CompactWorkQueueImpl>(
            s3adapterManager_, s3infoCache_, opts_);
        inited_ = true;
    } else {
        LOG(INFO) << "s3compact: not enabled";
    }
}

void S3CompactManager::RegisterS3Compact(std::weak_ptr<S3Compact> s3compact) {
    WriteLockGuard l(rwLock_);
    s3compacts_.emplace_back(s3compact);
}

int S3CompactManager::Run() {
    if (!inited_) {
        LOG(WARNING) << "s3compact: not inited, wont't run";
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
        s3adapterManager_->Deinit();
    }
}

void S3CompactManager::Enqueue() {
    std::vector<std::weak_ptr<S3Compact>> s3compacts;
    {
        // make a snapshot
        ReadLockGuard l(rwLock_);
        s3compacts = s3compacts_;
    }
    if (s3compacts.empty()) {
        // fastpath
        sleeper_.wait_for(std::chrono::milliseconds(opts_.enqueueSleepMS));
        return;
    }
    std::vector<int> toErase;
    for (int i = 0; i < s3compacts.size(); i++) {
        auto s3compact(s3compacts[i].lock());
        if (!s3compact) {
            // partition is deleted, record erase
            toErase.push_back(i);
            continue;
        }

        auto pinfo = s3compact->GetPartition();
        auto copysetNode = CopysetNodeManager::GetInstance().GetCopysetNode(
            pinfo.poolid(), pinfo.copysetid());
        // traverse inode container
        auto inodeManager = s3compact->GetInodeManager();

        std::list<uint64_t> inodes;
        inodeManager->GetInodeIdList(&inodes);
        uint64_t fsid = pinfo.fsid();

        if (inodes.empty()) {
            sleeper_.wait_for(std::chrono::milliseconds(opts_.enqueueSleepMS));
            continue;
        }
        for (const auto& inodeid : inodes) {
            sleeper_.wait_for(std::chrono::milliseconds(opts_.enqueueSleepMS));
            s3compactworkqueueImpl_->Enqueue(
                inodeManager, Key4Inode(fsid, inodeid), pinfo, copysetNode);
        }
    }
    {
        WriteLockGuard l(rwLock_);
        auto it = toErase.rbegin();
        while (it != toErase.rend()) {
            s3compacts_.erase(s3compacts_.begin() + *it);
            it++;
        }
    }
}

}  // namespace metaserver
}  // namespace curvefs

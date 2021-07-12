/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: April  13th 2021
 * Author: huyao
 */

#include "src/chunkserver/scan_manager.h"
#include "src/chunkserver/op_request.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::util::MessageDifferencer;

int ScanManager::Init(const ScanManagerOptions &options) {
    toStop_.store(false, std::memory_order_release);
    scanSize_ = options.scanSize;
    chunkMetaPageSize_ = options.chunkMetaPageSize;
    timeoutMs_ = options.timeoutMs;
    retry_ = options.retry;
    retryIntervalUs_ = options.retryIntervalUs;
    jobWaitInterval_.Init(options.intervalSec * 1000);
    // reuse timeout 1000ms as send scan task interval
    scanTaskWaitInterval_.Init(options.timeoutMs);
    copysetNodeManager_ = options.copysetNodeManager;
    chunkSize_ = copysetNodeManager_->GetCopysetNodeOptions().maxChunkSize;
    if (scanSize_ > chunkSize_ || scanSize_ <= 0 ||
        chunkSize_ % scanSize_ != 0) {
        LOG(ERROR) << "Init scan manager failed, "
                   << "the scan size: " << scanSize_;
        return -1;
    }
    return 0;
}

int ScanManager::Fini() {
    LOG(INFO) << "Stopping scan manager.";
    jobWaitInterval_.StopWait();
    toStop_.store(true, std::memory_order_release);
    scanThread_.join();
    waitScanSet_.clear();
    jobs_.clear();
    LOG(INFO) << "Stopped scan manager.";
    return 0;
}

void ScanManager::Enqueue(LogicPoolID poolId, CopysetID id) {
    ScanKey key(poolId, id);
    WriteLockGuard writeGuard(waitSetLock_);
    if (waitScanSet_.find(key) == waitScanSet_.end()) {
        waitScanSet_.emplace(key);
    } else {
        LOG(WARNING) << "The scan key already exists"
                     << "logical poolId: " << poolId
                     << "copysetId: " << id;
    }
}

ScanKey ScanManager::Dequeue() {
    ScanKey key;
    WriteLockGuard writeGuard(waitSetLock_);
    key = *(waitScanSet_.begin());
    waitScanSet_.erase(waitScanSet_.begin());
    return key;
}

int ScanManager::Run() {
    scanThread_ = Thread(&ScanManager::Scan, this);
    return 0;
}

void ScanManager::Scan() {
    LOG(INFO) << "Starting Scan worker thread.";
    ScanKey key;
    while (!toStop_.load(std::memory_order_acquire)) {
        waitSetLock_.RDLock();
        if (waitScanSet_.empty()) {
            waitSetLock_.Unlock();
            jobWaitInterval_.WaitForNextExcution();
            continue;
        }
        waitSetLock_.Unlock();
        key = Dequeue();
        StartScanJob(key);
        jobWaitInterval_.WaitForNextExcution();
    }
    LOG(INFO) << "Scan worker thread stopped.";
}

void ScanManager::StartScanJob(ScanKey key) {
    // check copyset exist
    auto nodePtr = copysetNodeManager_->GetCopysetNode(key.first, key.second);
    if (nullptr == nodePtr) {
        LOG(ERROR) << "scan copyset failed, copyset node is not found:"
                   << " logicalpoolId = " << key.first
                   << " copysetId = " << key.second;
        return;
    }

    LOG(INFO) << "Start scan job(" << key.first
              << ", " << key.second << ").";
    std::shared_ptr<ScanJob> job = std::make_shared<ScanJob>();
    job->poolId = key.first;
    job->id = key.second;
    job->type = ScanType::Init;
    job->isFinished = true;
    job->dataStore = nodePtr->GetDataStore();
    nodePtr->SetScan(true);
    nodePtr->GetFailedScanMap().clear();
    jobMapLock_.WRLock();
    jobs_.emplace(key, job);
    jobMapLock_.Unlock();
    GenScanJobs(key);
}

int ScanManager::CancelScanJob(LogicPoolID poolId, CopysetID id) {
    ScanKey key(poolId, id);
    // cancel scan job not started
    {
        WriteLockGuard writeGuard(waitSetLock_);
        auto iter = waitScanSet_.find(key);
        if (iter != waitScanSet_.end()) {
            waitScanSet_.erase(iter);
            return 0;
        }
    }
    // cancel scan job started
    auto job = GetJob(key);
    if (nullptr != job) {
        auto nodePtr = copysetNodeManager_->GetCopysetNode(poolId, id);
        nodePtr->SetScan(false);
        nodePtr->GetFailedScanMap().clear();
        WriteLockGuard writeGuard(jobMapLock_);
        jobs_.erase(key);
    }
    return 0;
}

bool ScanManager::GenScanJob(std::shared_ptr<ScanJob> job) {
    bool done = false;
    switch (job->type) {
        case ScanType::Init:
            job->chunkMap = job->dataStore->GetChunkMap();
            job->type = ScanType::NewMap;
            break;
        case ScanType::NewMap:
            if (0 == ScanJobProcess(job)) {
                job->type = ScanType::Finish;
                break;
            }
            done = true;
            break;
        case ScanType::WaitMap:
            {
                ReadLockGuard readGuard(job->taskLock);
                if (0 == job->task.waitingNum) {
                    job->type = ScanType::CompareMap;
                    break;
                }
            }
            done = true;
            break;
        case ScanType::CompareMap:
            CompareMap(job);
            done = true;
            break;
        case ScanType::Finish:
            ScanJobFinish(job);
            done = true;
            break;
        default:
            LOG(WARNING) << "Error scan type.";
            break;
    }
    return done;
}

void ScanManager::GenScanJobs(ScanKey key) {
    bool done = false;
    auto job = GetJob(key);
    if (nullptr == job) {
        LOG(ERROR) << "GenScanJon failed, can not find the job,"
                   << " logical poolId = " << key.first
                   << " copysetId = " << key.second;
        return;
    }

    while (!done) {
        done = GenScanJob(job);
    }
}

// send scan request to braft
int ScanManager::ScanJobProcess(const std::shared_ptr<ScanJob> job) {
    // check chunkmap
    if (job->chunkMap.empty()) {
        LOG(WARNING) << "GenScanJob failed, job's chunkmap is empty"
                     << " logicalpoolId = " << job->poolId
                     << " copysetId = " << job->id;
        return 0;
    }

    // iterate chunkmap
    auto nodePtr = copysetNodeManager_->GetCopysetNode(job->poolId, job->id);
    std::vector<Peer> peers;
    nodePtr->ListPeers(&peers);
    auto replicaNum = peers.size();
    auto iter = job->chunkMap.begin();
    while (iter != job->chunkMap.end()) {
        // check chunk version
        auto csChunkFile = iter->second;
        if (csChunkFile->GetChunkFileMetaPage().version !=
            FORMAT_VERSION_V2) {
            iter++;
        } else {
            // split scan chunk request
            uint32_t currentOffset = 0;
            bool scanChunkMetaPage = true;
            while (currentOffset < chunkSize_) {
                // check is leader, if not cancel the job
                if (!nodePtr->IsLeaderTerm()) {
                    CancelScanJob(job->poolId, job->id);
                    return -1;
                }

                // Init job
                job->taskLock.WRLock();
                job->task.localMap.Clear();
                job->task.followerMap.clear();
                job->task.waitingNum = replicaNum;
                job->task.chunkId = iter->first;
                job->task.offset = currentOffset;
                if (scanChunkMetaPage) {
                    job->task.len = chunkMetaPageSize_;
                } else {
                    job->task.len = scanSize_;
                }
                job->taskLock.Unlock();
                job->isFinished = false;

                // construct scan task
                ChunkResponse *response = new ChunkResponse();
                ChunkRequest *request = new ChunkRequest();
                request->set_optype(CHUNK_OP_TYPE::CHUNK_OP_SCAN);
                request->set_logicpoolid(job->poolId);
                request->set_copysetid(job->id);
                request->set_chunkid(iter->first);
                request->set_offset(currentOffset);
                request->set_sendscanmaptimeoutms(timeoutMs_);
                request->set_sendscanmapretrytimes(retry_);
                request->set_sendscanmapretryintervalus(retryIntervalUs_);
                if (scanChunkMetaPage) {
                    request->set_readmetapage(true);
                    request->set_size(chunkMetaPageSize_);
                } else {
                    request->set_size(scanSize_);
                }
                ScanChunkClosure *done = new ScanChunkClosure(request,
                                                              response);
                std::shared_ptr<ScanChunkRequest> req =
                    std::make_shared<ScanChunkRequest>(nodePtr, this, request,
                                                    response, done);
                req->Process();
                if (!scanChunkMetaPage) {
                    currentOffset += scanSize_;
                }
                // wait for scan task finished
                uint32_t retry = retry_;
                while (!job->isFinished && retry > 0) {
                    scanTaskWaitInterval_.WaitForNextExcution();
                    retry--;
                }
                scanChunkMetaPage = false;
            }
            iter++;
        }
    }
    return 0;
}

void ScanManager::SetLocalScanMap(ScanKey key, ScanMap map) {
    auto job = GetJob(key);
    if (nullptr == job) {
        LOG(WARNING) << "SetLocalScanMap failed, job not found,"
                     << " logical poolId = " << key.first
                     << " copysetId = " << key.second;
        return;
    }
    job->taskLock.RDLock();
    bool matched = job->task.chunkId == map.chunkid() &&
                   job->task.offset == map.offset() &&
                   job->task.len == map.len();
    job->taskLock.Unlock();

    if (!matched) {
        LOG(WARNING) << "SetLocalScanMap failed, mismatch scanmap."
                     << " scantask.chunkid = " << job->task.chunkId
                     << " scantask.offset = " << job->task.offset
                     << " scantask.len = " << job->task.len
                     << "; scanmap: " << map.ShortDebugString();
        return;
    }

    WriteLockGuard writeLockGuard(job->taskLock);
    job->task.localMap = map;
    job->task.waitingNum--;
    LOG(INFO) << "Leader scanmap is: " << job->task.localMap.ShortDebugString();
}

void ScanManager::DealFollowerScanMap(const FollowScanMapRequest &request,
                                      FollowScanMapResponse *response) {
    const ScanMap &scanMap = request.scanmap();
    ScanKey key(scanMap.logicalpoolid(), scanMap.copysetid());
    auto job = GetJob(key);

    if (nullptr == job) {
        LOG(WARNING) << "DealFollowerScanMap failed, job not found."
                     << " logical poolId = " << key.first
                     << " copysetId = " << key.second;
        response->set_retcode(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
        return;
    }

    job->taskLock.RDLock();
    bool matched = job->task.chunkId == scanMap.chunkid() &&
                   job->task.offset == scanMap.offset() &&
                   job->task.len == scanMap.len();
    job->taskLock.Unlock();

    if (matched) {
        job->taskLock.WRLock();
        job->task.followerMap.emplace_back(scanMap);
        job->task.waitingNum--;
        job->taskLock.Unlock();
        job->type = ScanType::WaitMap;
        GenScanJobs(key);
        response->set_retcode(CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS);
        return;
    } else {
        ReadLockGuard readLockGuard(job->taskLock);
        LOG(WARNING) << "DealFollowerScanMap failed, mismatch scanmap."
                     << " scantask.chunkid = " << job->task.chunkId
                     << " scantask.offset = " << job->task.offset
                     << " scantask.len = " << job->task.len
                     << "; scanmap: " << scanMap.ShortDebugString();
    }
    response->set_retcode(CHUNK_OP_STATUS::CHUNK_OP_STATUS_INVALID_REQUEST);
}

void ScanManager::CompareMap(std::shared_ptr<ScanJob> job) {
    if (nullptr != job) {
        WriteLockGuard writeLock(job->taskLock);
        if (0 == job->task.waitingNum) {
            // check there are three scanmap in scanmap unit
            if (job->task.followerMap.size() != 2 ||
                !job->task.localMap.IsInitialized()) {
                LOG(ERROR) << "Compare scanmap failed on chunkId = "
                           << job->task.chunkId
                           << " offset = " << job->task.offset
                           << " , because waitingNum is 0"
                           << " but there isn't three scanmap."
                           << " the follower scanmap size = "
                           << job->task.followerMap.size()
                           << " the leader scanmap initialied = "
                           << job->task.localMap.IsInitialized();
            } else if (!(MessageDifferencer::Equals(job->task.localMap,
                                             job->task.followerMap[0]) &&
                       MessageDifferencer::Equals(job->task.localMap,
                                                  job->task.followerMap[1]))) {
                LOG(ERROR) << "Compare scanmap failed,"
                           << " the leader scanmap: "
                           << job->task.localMap.ShortDebugString()
                           << "; the first follower scanmap: "
                           << job->task.followerMap[0].ShortDebugString()
                           << "; the second follower scanmap: "
                           << job->task.followerMap[1].ShortDebugString();
                // set failed scanmap
                auto nodePtr = copysetNodeManager_->GetCopysetNode(job->poolId,
                                                                   job->id);
                nodePtr->GetFailedScanMap().emplace_back(job->task.localMap);
            } else {
                LOG(INFO) << "Compare scanmap successfully on"
                          << " logicalpoolId = " << job->poolId
                          << " copysetId = " << job->id
                          << " chunkId = " << job->task.chunkId
                          << " offset = " << job->task.offset
                          << " len = " << job->task.len;
            }
            job->isFinished = true;
        }
    }
}

void ScanManager::ScanJobFinish(std::shared_ptr<ScanJob> job) {
    if (nullptr != job) {
        ScanKey key(job->poolId, job->id);
        auto nodePtr = copysetNodeManager_->GetCopysetNode(job->poolId,
                                                           job->id);
        uint64_t now = ::curve::common::TimeUtility::GetTimeofDaySec();
        nodePtr->SetLastScan(now);
        nodePtr->SetScan(false);
        WriteLockGuard writeGuard(jobMapLock_);
        jobs_.erase(key);
        LOG(INFO) << "Scan job (" << key.first << ", "
                  << key.second << ") finished.";
    }
}

std::shared_ptr<ScanJob> ScanManager::GetJob(ScanKey key) {
    ReadLockGuard readGuard(jobMapLock_);
    auto iter = jobs_.find(key);
    if (jobs_.end() != iter) {
        return iter->second;
    }
    return nullptr;
}

void ScanManager::SetScanJobType(ScanKey key, ScanType type) {
    auto job = GetJob(key);
    if (nullptr != job) {
        job->type = type;
    }
}

}  // namespace chunkserver
}  // namespace curve

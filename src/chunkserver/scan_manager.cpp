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

namespace curve {
namespace chunkserver {

ScanManager::~ScanManager() {
    return;
}

int ScanManager::Init(const ScanManagerOptions options) {
    toStop_.store(false, std::memory_order_release);
    scanSize = options.scanSize;
    scanThread_ = Thread(&ScanManager::Run, this);
    waitInterval_.Init(options.intervalSec * 1000);
    return 0;
}

int ScanManager::Fini() {
    waitInterval_.StopWait();
    return 0;
}

int ScanManager::Run() {
    ScanKey key;
    while (!toStop_.load(std::memory_order_acquire)) {
        rwLock_.RDLock();
        if (waitScanSet.empty()) {
            rwLock_.Unlock();
            waitInterval_.WaitForNextExcution();
            continue;
        }
        rwLock_.Unlock();
        key = Dequeue();
        StartScanJob(key);
    }

    return 0;
}

void ScanManager::Enqueue(LogicPoolID poolId, CopysetID id) {
    WriteLockGuard writeGuard(rwLock_);
    ScanKey key(poolId, id);
    if (waitScanSet.find(key) == waitScanSet.end()) {
        waitScanSet.insert(key);
    }
    return;
}

ScanKey ScanManager::Dequeue() {
    ScanKey key;
    WriteLockGuard writeGuard(rwLock_);
    key = *(waitScanSet.begin());
    waitScanSet.erase(waitScanSet.begin());
    return key;
}

int ScanManager::CancelScanJob(LogicPoolID poolId, CopysetID id) {
    return 0;
}

bool ScanManager::IsRepeatReq(LogicPoolID poolId, CopysetID id) {
    return 0;
}

void ScanManager::StartScanJob(ScanKey key) {
    job_.poolId = key.first;
    job_.id = key.second;
    job_.type = ScanType::Init;
    job_.currentChunkId = 0;
    job_.startTime = 0;
    GenScanJob();
}

void ScanManager::GenScanJob() {
    bool done = false;

    while (!done) {
        switch (job_.type) {
            case ScanType::Init:
                chunkMap_ = dataStore_->GetChunkMap();
                job_.type = ScanType::NewMap;
                break;
            case ScanType::NewMap:
                {
                    ChunkMap::iterator iter;
                    iter = chunkMap_.find(job_.currentChunkId);
                    if (iter == chunkMap_.end()) {
                        job_.currentChunkId = chunkMap_.begin()->first;
                    }

                    ScanChunkReqProcess();
                    done = true;
                    break;
                }
            case ScanType::BuildMap:
                BuildLocalScanMap();
                job_.type = ScanType::WaitRep;
                done = true;
                break;
            case ScanType::WaitRep:
                if (job_.waitingNum == 0) {
                    job_.type = ScanType::CompareMap;
                    break;
                }
                done = true;
                break;
            case ScanType::CompareMap:
                CompareMap();
                if (isCurrentJobFinish()) {
                    job_.type = ScanType::Finish;
                } else {
                    job_.type = ScanType::NewMap;
                    job_.currentChunkId = GetNextScanChunk();
                }
                break;
            case ScanType::Finish:
                ScanJobFinish();
                done = true;
                break;
            default:
                break;
        }
    }
}

ChunkID ScanManager::GetNextScanChunk() {
    return 1;
}

void ScanManager::ScanError() {
    return;
}

void ScanManager::ScanChunkReqProcess() {
    return;
}

void ScanManager::BuildLocalScanMap() {
    return;
}

void ScanManager::ScanJobFinish() {
    return;
}

void ScanManager::CompareMap() {
    return;
}

bool ScanManager::isCurrentJobFinish() {
    return true;
}
}  // namespace chunkserver
}  // namespace curve

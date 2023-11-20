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
 * Project: nebd
 * Created Date: Friday February 14th 2020
 * Author: yangyaokai
 */

#include "nebd/src/part2/heartbeat_manager.h"

#include <string>
#include <unordered_map>

#include "nebd/src/common/timeutility.h"

namespace nebd {
namespace server {

using nebd::common::TimeUtility;

int HeartbeatManager::Run() {
    if (isRunning_.exchange(true)) {
        LOG(WARNING) << "heartbeat manager is on running.";
        return -1;
    }

    checkTimeoutThread_ =
        std::thread(&HeartbeatManager::CheckTimeoutFunc, this);
    LOG(INFO) << "Run heartbeat manager success.";
    return 0;
}

int HeartbeatManager::Fini() {
    if (isRunning_.exchange(false)) {
        LOG(INFO) << "Stopping heartbeat manager...";
        sleeper_.interrupt();
        checkTimeoutThread_.join();
    }
    LOG(INFO) << "Stop heartbeat manager success.";
    return 0;
}

bool HeartbeatManager::UpdateFileTimestamp(int fd, uint64_t timestamp) {
    NebdFileEntityPtr entity = fileManager_->GetFileEntity(fd);
    if (entity == nullptr) {
        LOG(ERROR) << "File not exist, fd: " << fd;
        return false;
    }
    entity->UpdateFileTimeStamp(timestamp);
    return true;
}

void HeartbeatManager::UpdateNebdClientInfo(int pid, const std::string& version,
                                            uint64_t timestamp) {
    WriteLockGuard writeLock(rwLock_);
    const auto& iter = nebdClients_.find(pid);
    if (iter == nebdClients_.end()) {
        nebdClients_[pid] =
            std::make_shared<NebdClientInfo>(pid, version, timestamp);
        nebdClientNum_ << 1;
    } else {
        nebdClients_[pid]->timeStamp = timestamp;
        nebdClients_[pid]->version.Set(kVersion, version);
        nebdClients_[pid]->version.Update();
    }
}

void HeartbeatManager::CheckTimeoutFunc() {
    while (
        sleeper_.wait_for(std::chrono::milliseconds(checkTimeoutIntervalMs_))) {
        LOG_EVERY_N(INFO, 60 * 1000 / checkTimeoutIntervalMs_)
            << "Checking timeout, file status: "
            << fileManager_->DumpAllFileStatus();
        FileEntityMap fileEntityMap = fileManager_->GetFileEntityMap();
        NebdFileEntityPtr curEntity;
        for (const auto& entityPair : fileEntityMap) {
            curEntity = entityPair.second;
            bool needClose = CheckNeedClosed(curEntity);
            if (!needClose) {
                continue;
            }
            std::string standardTime;
            TimeUtility::TimeStampToStandard(
                curEntity->GetFileTimeStamp() / 1000, &standardTime);
            LOG(INFO) << "Close file which has timed out. "
                      << "Last time received heartbeat or request: "
                      << standardTime;
            curEntity->Close(false);
        }
        RemoveTimeoutNebdClient();
    }
}

bool HeartbeatManager::CheckNeedClosed(NebdFileEntityPtr entity) {
    uint64_t curTime = TimeUtility::GetTimeofDayMs();
    uint64_t interval = curTime - entity->GetFileTimeStamp();
    // If the file is in an open state and has timed out, you need to call close
    bool needClose = entity->GetFileStatus() == NebdFileStatus::OPENED &&
                     interval > (uint64_t)1000 * heartbeatTimeoutS_;
    return needClose;
}

std::ostream& operator<<(std::ostream& os, NebdClientInfo* info) {
    std::string standardTime;
    TimeUtility::TimeStampToStandard(info->timeStamp / 1000, &standardTime);
    os << "pid: " << info->pid
       << ", version: " << info->version.GetValueByKey(kVersion)
       << ", last time received heartbeat: " << standardTime;
    return os;
}

void HeartbeatManager::RemoveTimeoutNebdClient() {
    WriteLockGuard writeLock(rwLock_);
    auto iter = nebdClients_.begin();
    while (iter != nebdClients_.end()) {
        uint64_t curTime = TimeUtility::GetTimeofDayMs();
        uint64_t interval = curTime - iter->second->timeStamp;
        if (interval > (uint64_t)1000 * heartbeatTimeoutS_) {
            LOG(INFO) << "Delete nebd client info which has timed out. "
                      << "client info: " << iter->second;
            iter = nebdClients_.erase(iter);
            nebdClientNum_ << -1;
        } else {
            iter++;
        }
    }
}

}  // namespace server
}  // namespace nebd

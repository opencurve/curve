/*
 * Project: nebd
 * Created Date: Friday February 14th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include <unordered_map>
#include <string>

#include "src/common/timeutility.h"
#include "src/part2/heartbeat_manager.h"

namespace nebd {
namespace server {

using common::TimeUtility;

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

void HeartbeatManager::CheckTimeoutFunc() {
    while (sleeper_.wait_for(
        std::chrono::milliseconds(checkTimeoutIntervalMs_))) {
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
    }
}

bool HeartbeatManager::CheckNeedClosed(NebdFileEntityPtr entity) {
    uint64_t curTime = TimeUtility::GetTimeofDayMs();
    uint64_t interval = curTime - entity->GetFileTimeStamp();
    // 文件如果是opened状态，并且已经超时，则需要调用close
    bool needClose = entity->GetFileStatus() == NebdFileStatus::OPENED
                     && interval > (uint64_t)1000 * heartbeatTimeoutS_;
    return needClose;
}

}  // namespace server
}  // namespace nebd

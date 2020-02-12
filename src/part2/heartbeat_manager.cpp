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

int HeartbeatManager::UpdateFileTimestamp(int fd, uint64_t timestamp) {
    return fileManager_->GetRecordManager()->UpdateFileTimestamp(fd, timestamp);
}

void HeartbeatManager::CheckTimeoutFunc() {
    while (sleeper_.wait_for(
        std::chrono::milliseconds(checkTimeoutIntervalMs_))) {
        FileRecordManagerPtr recordManager = fileManager_->GetRecordManager();
        FileRecordMap fileRecords = recordManager->ListRecords();
        LOG_EVERY_N(INFO, 60 * 1000 / checkTimeoutIntervalMs_)
            << "Checking timeout, file records num: " << fileRecords.size();
        for (auto& fileRecord : fileRecords) {
            bool needClose = CheckNeedClosed(fileRecord.first);
            if (!needClose) {
                continue;
            }
            std::string standardTime;
            TimeUtility::TimeStampToStandard(
                fileRecord.second.timeStamp / 1000, &standardTime);
            LOG(INFO) << "Close file which has timed out. "
                      << "Last time received heartbeat or request: "
                      << standardTime;
            fileManager_->Close(fileRecord.first, false);
        }
    }
}

bool HeartbeatManager::CheckNeedClosed(int fd) {
    FileRecordManagerPtr recordManager = fileManager_->GetRecordManager();
    NebdFileRecord record;
    bool getTimeSuccess = recordManager->GetRecord(fd, &record);
    if (!getTimeSuccess) {
        return false;
    }

    uint64_t curTime = TimeUtility::GetTimeofDayMs();
    uint64_t interval = curTime - record.timeStamp;
    // 文件如果是opened状态，并且已经超时，则需要调用close
    bool needClose = record.status == NebdFileStatus::OPENED
                     && interval > (uint64_t)1000 * heartbeatTimeoutS_;
    return needClose;
}

}  // namespace server
}  // namespace nebd

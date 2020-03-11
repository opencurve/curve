/**
 * Project : curve
 * Created Date : 2020-03-13
 * Author : wuhanqing
 * Copyright (c) 2018 netease
 */

#include "src/mds/nameserver2/file_record.h"

namespace curve {
namespace mds {

void FileRecordManager::Init(const FileRecordOptions& fileRecordOptions) {
    fileRecordOptions_ = fileRecordOptions;
}

void FileRecordManager::Start() {
    scanThread_ = curve::common::Thread(&FileRecordManager::Scan, this);
    running_ = true;
}

void FileRecordManager::Stop() {
    if (running_.exchange(false)) {
        LOG(INFO) << "stop FileRecordManager...";
        sleeper_.interrupt();

        scanThread_.join();
        LOG(INFO) << "stop FileRecordManager success";
    }
}

void FileRecordManager::UpdateFileRecord(const std::string& fileName) {
    do {
        ReadLockGuard lk(rwlock_);

        auto it = fileRecords_.find(fileName);
        if (it == fileRecords_.end()) {
            break;
        }

        // 更新record
        it->second.Update();
        return;
    } while (0);

    FileRecord record(fileRecordOptions_.fileRecordExpiredTimeUs);
    WriteLockGuard lk(rwlock_);
    fileRecords_.emplace(fileName, record);
}

void FileRecordManager::Scan() {
    while (sleeper_.wait_for(
            std::chrono::microseconds(fileRecordOptions_.scanIntervalTimeUs))) {
        WriteLockGuard lk(rwlock_);

        auto iter = fileRecords_.begin();
        while (iter != fileRecords_.end()) {
            if (iter->second.IsTimeout()) {
                iter = fileRecords_.erase(iter);
            } else {
                ++iter;
            }
        }
    }
}

void FileRecordManager::GetRecordParam(ProtoSession* protoSession) const {
    protoSession->set_sessionid("");
    protoSession->set_leasetime(fileRecordOptions_.fileRecordExpiredTimeUs);
    protoSession->set_createtime(
        curve::common::TimeUtility::GetTimeofDayUs());
    protoSession->set_sessionstatus(SessionStatus::kSessionOK);
}

}  // namespace mds
}  // namespace curve

/**
 * Project : curve
 * Created Date : 2020-03-13
 * Author : wuhanqing
 * Copyright (c) 2018 netease
 */

#include "src/mds/nameserver2/file_record.h"
#include "src/mds/common/mds_define.h"

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

bool FileRecordManager::GetFileClientVersion(
    const std::string& fileName, std::string *clientVersion) const {
    ReadLockGuard lk(rwlock_);

    auto it = fileRecords_.find(fileName);
    if (it == fileRecords_.end()) {
        return false;
    }

    *clientVersion = it->second.GetClientVersion();
    return true;
}

void FileRecordManager::UpdateFileRecord(const std::string& fileName,
                                         const std::string& clientVersion,
                                         const std::string& clientIP,
                                         uint32_t clientPort) {
    do {
        ReadLockGuard lk(rwlock_);

        auto it = fileRecords_.find(fileName);
        if (it == fileRecords_.end()) {
            break;
        }

        // 更新record
        it->second.Update(clientVersion, clientIP, clientPort);
        return;
    } while (0);

    FileRecord record(fileRecordOptions_.fileRecordExpiredTimeUs,
                      clientVersion,
                      clientIP,
                      clientPort);
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

std::set<ClientIpPortType> FileRecordManager::ListAllClient() const {
    std::set<ClientIpPortType> res;

    {
        ReadLockGuard lk(rwlock_);
        for (const auto& r : fileRecords_) {
            const auto& ipPort = r.second.GetClientIpPort();
            if (ipPort.second != kInvalidPort) {
                res.emplace(ipPort);
            }
        }
    }

    return res;
}

bool FileRecordManager::FindFileMountPoint(const std::string& fileName,
                                           ClientIpPortType* ipPort) const {
    ReadLockGuard lk(rwlock_);
    auto iter = fileRecords_.find(fileName);
    if (iter == fileRecords_.end()) {
        return false;
    }

    *ipPort = iter->second.GetClientIpPort();
    return true;
}

}  // namespace mds
}  // namespace curve

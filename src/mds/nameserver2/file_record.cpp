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

/**
 * Project : curve
 * Created Date : 2020-03-13
 * Author : wuhanqing
 */

#include "src/mds/nameserver2/file_record.h"

#include <algorithm>
#include <map>
#include <vector>

#include "src/common/timeutility.h"
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

bool FileRecordManager::GetMinimumFileClientVersion(
    const std::string& fileName, std::string *clientVersion) const {
    ReadLockGuard lk(rwlock_);

    auto it = fileRecords_.find(fileName);
    if (it == fileRecords_.end()) {
        return false;
    }

    auto& files = it->second;
    if (files.empty()) {
        return false;
    }

    std::string mini = files.begin()->second.GetClientVersion();
    for (auto& r : files) {
        mini = std::min(mini, r.second.GetClientVersion());
    }

    *clientVersion = std::move(mini);
    return true;
}

void FileRecordManager::UpdateFileRecord(const std::string& fileName,
                                         const std::string& clientVersion,
                                         const std::string& clientIP,
                                         uint32_t clientPort) {
    butil::EndPoint clientEp;
    int rc = butil::str2endpoint(clientIP.c_str(), clientPort, &clientEp);
    if (rc != 0) {
        LOG(ERROR) << "Failed to UpdateFileRecord, invalid ip:port, filename: "
                   << fileName << ", ip: " << clientIP
                   << ", port: " << clientPort;
        return;
    }

    do {
        ReadLockGuard lk(rwlock_);

        auto it = fileRecords_.find(fileName);
        if (it == fileRecords_.end()) {
            break;
        }

        auto recordIter = it->second.find(clientEp);
        if (recordIter == it->second.end()) {
            break;
        }

        // update record
        recordIter->second.Update(clientVersion, clientEp);
        return;
    } while (0);

    FileRecord record(fileRecordOptions_.fileRecordExpiredTimeUs, clientVersion,
                      clientEp);
    LOG(INFO) << "Add new file record, filename = " << fileName
              << ", clientVersion = " << clientVersion << ", client endpoint = "
              << butil::endpoint2str(clientEp).c_str();

    WriteLockGuard lk(rwlock_);
    if (fileRecords_.count(fileName) == 0) {
        fileRecords_.emplace(fileName, std::map<butil::EndPoint, FileRecord>{});
    }

    fileRecords_[fileName].emplace(clientEp, record);
}

void FileRecordManager::RemoveFileRecord(const std::string& filename,
                                         const std::string& clientIp,
                                         uint32_t clientPort) {
    butil::EndPoint ep;
    int rc = butil::str2endpoint(clientIp.c_str(), clientPort, &ep);
    if (rc != 0) {
        LOG(ERROR) << "Failed to RemoveFileRecord, invalid ip:port, filename: "
                   << filename << ", ip: " << clientIp
                   << ", port: " << clientPort;
        return;
    }

    WriteLockGuard lk(rwlock_);
    auto it = fileRecords_.find(filename);
    if (it == fileRecords_.end()) {
        return;
    }

    it->second.erase(ep);
}

void FileRecordManager::Scan() {
    while (sleeper_.wait_for(
        std::chrono::microseconds(fileRecordOptions_.scanIntervalTimeUs))) {
        WriteLockGuard lk(rwlock_);

        auto iter = fileRecords_.begin();
        while (iter != fileRecords_.end()) {
            auto recordIter = iter->second.begin();
            while (recordIter != iter->second.end()) {
                if (recordIter->second.IsTimeout()) {
                    LOG(INFO)
                        << "Remove timeout file record, filename = "
                        << iter->first << ", last update time = "
                        << curve::common::TimeUtility::TimeStampToStandard(
                               recordIter->second.GetUpdateTime() / 1000000)
                        << ", endpoint = "
                        << butil::endpoint2str(
                               recordIter->second.GetClientEndPoint())
                               .c_str();
                    recordIter = iter->second.erase(recordIter);
                } else {
                    ++recordIter;
                }
            }

            if (iter->second.empty()) {
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

std::set<butil::EndPoint> FileRecordManager::ListAllClient() const {
    std::set<butil::EndPoint> res;

    {
        ReadLockGuard lk(rwlock_);
        for (const auto& files : fileRecords_) {
            for (const auto& r : files.second) {
                const auto& ep = r.second.GetClientEndPoint();
                if (ep.port != kInvalidPort) {
                    res.emplace(ep);
                }
            }
        }
    }

    return res;
}

bool FileRecordManager::FindFileMountPoint(
    const std::string& fileName, std::vector<butil::EndPoint>* eps) const {
    ReadLockGuard lk(rwlock_);
    auto iter = fileRecords_.find(fileName);
    if (iter == fileRecords_.end()) {
        return false;
    }

    for (auto& f : iter->second) {
        eps->emplace_back(f.second.GetClientEndPoint());
    }

    return true;
}

}  // namespace mds
}  // namespace curve

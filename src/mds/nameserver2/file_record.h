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

#ifndef SRC_MDS_NAMESERVER2_FILE_RECORD_H_
#define SRC_MDS_NAMESERVER2_FILE_RECORD_H_

#include <unordered_map>
#include <utility>
#include <string>
#include <set>

#include "src/common/concurrent/rw_lock.h"
#include "src/common/interruptible_sleeper.h"
#include "src/common/timeutility.h"
#include "proto/nameserver2.pb.h"

namespace curve {
namespace mds {

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;
using ClientIpPortType = std::pair<std::string, uint32_t>;

struct FileRecordOptions {
    // file record expire time (in μs)
    uint32_t fileRecordExpiredTimeUs;
    // time interval of scanning file record map (in μs)
    uint32_t scanIntervalTimeUs;
};

class FileRecord {
 public:
    FileRecord(uint64_t timeoutUs, const std::string& clientVersion,
               const std::string& clientIP, uint32_t clientPort)
        : updateTimeUs_(curve::common::TimeUtility::GetTimeofDayUs()),
          timeoutUs_(timeoutUs),
          clientVersion_(clientVersion),
          clientIP_(clientIP),
          clientPort_(clientPort) {}

    FileRecord(const FileRecord& fileRecord)
        : updateTimeUs_(fileRecord.updateTimeUs_),
          timeoutUs_(fileRecord.timeoutUs_),
          clientVersion_(fileRecord.clientVersion_),
          clientIP_(fileRecord.clientIP_),
          clientPort_(fileRecord.clientPort_) {}

    FileRecord& operator=(const FileRecord& fileRecord) {
        updateTimeUs_ = fileRecord.updateTimeUs_;
        timeoutUs_ = fileRecord.timeoutUs_;
        clientVersion_ = fileRecord.clientVersion_;
        clientIP_ = fileRecord.clientIP_;
        clientPort_ = fileRecord.clientPort_;
        return *this;
    }

    /**
     * @brief determine whether the request has timeout
     * @return true if timeout, false if not
     */
    bool IsTimeout() const {
        curve::common::LockGuard lk(mtx_);
        uint64_t currentTimeUs = curve::common::TimeUtility::GetTimeofDayUs();
        return currentTimeUs > updateTimeUs_ + 10 * timeoutUs_;
    }

    /**
     * @brief Update time
     */
    void Update(const std::string& clientVersion, const std::string& clientIP,
                uint32_t clientPort) {
        curve::common::LockGuard lk(mtx_);
        updateTimeUs_ = curve::common::TimeUtility::GetTimeofDayUs();
        clientVersion_ = clientVersion;
        clientIP_ = clientIP;
        clientPort_ = clientPort_;
    }

    /**
     * @brief Update version number
     */
    void Update(const std::string& clientVersion) {
        curve::common::LockGuard lk(mtx_);
        clientVersion_ = clientVersion;
    }

    /**
     * @brief Get the time last updated
     * @return last updated time
     */
    uint64_t GetUpdateTime() const {
        curve::common::LockGuard lk(mtx_);
        return updateTimeUs_;
    }

    /**
     * @brief Get the client version of currently opened file
     * @return version number, null if has no version
     */
    std::string GetClientVersion() const {
        return clientVersion_;
    }

    ClientIpPortType GetClientIpPort() const {
        return {clientIP_, clientPort_};
    }

 private:
    // latest update time in μs
    uint64_t updateTimeUs_;
    // timeout
    uint64_t timeoutUs_;
    // client version
    std::string clientVersion_;
    // client IP address
    std::string clientIP_;
    // client port
    uint32_t clientPort_;
    // mutex for updating the time
    mutable curve::common::Mutex mtx_;
};

class FileRecordManager {
 public:
    /**
     * @brief initialization
     * @param[in] sessionOption session configuration
     */
    void Init(const FileRecordOptions& fileRecordOptions);

    /**
     * @brief Get the opened file number
     * @return the number of the opened files
     */
    uint64_t GetOpenFileNum() const {
        ReadLockGuard lk(rwlock_);
        return fileRecords_.size();
    }

    /**
     * @brief Get the expired time of the file
     * @return the expired time
     */
    uint32_t GetFileRecordExpiredTimeUs() const {
        return fileRecordOptions_.fileRecordExpiredTimeUs;
    }

    /**
     * @brief Update the file record corresponding to the input filename
     * @param[in] filename
     */
    void UpdateFileRecord(const std::string& filename,
                          const std::string& clientVersion,
                          const std::string& clientIP,
                          uint32_t clientPort);

    /**
     * @brief Get the client version corresponding to the input file
     * @param[in] filename
     */
    bool GetFileClientVersion(
        const std::string& fileName, std::string *clientVersion) const;

    /**
     * @brief start FileRecordManager
     */
    void Start();

    /**
     * @brief stop FileRecordManager
     */
    void Stop();

    /**
     * @brief Set protoSession according to the configuration
     * @param[out] protoSession: Params to configure
     */
    void GetRecordParam(ProtoSession* protoSession) const;

    std::set<ClientIpPortType> ListAllClient() const;

    bool FindFileMountPoint(const std::string& fileName,
                            ClientIpPortType* ipPort) const;

 private:
    /**
     * @brief Function for periodic scanning, it deletes timed-out file records
     */
    void Scan();

    // file recoreds
    std::unordered_map<std::string, FileRecord> fileRecords_;
    // rwlock for fileRecords_
    mutable curve::common::RWLock rwlock_;
    // the thread for scanning in backend
    curve::common::Thread scanThread_;

    FileRecordOptions fileRecordOptions_;
    curve::common::InterruptibleSleeper sleeper_;
    curve::common::Atomic<bool> running_{false};
};

}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_FILE_RECORD_H_

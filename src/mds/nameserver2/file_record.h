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

#include <butil/endpoint.h>

#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "proto/nameserver2.pb.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/interruptible_sleeper.h"
#include "src/common/timeutility.h"

namespace curve {
namespace mds {

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

struct FileRecordOptions {
    // file record expire time (in μs)
    uint32_t fileRecordExpiredTimeUs;
    // time interval of scanning file record map (in μs)
    uint32_t scanIntervalTimeUs;
};

class FileRecord {
 public:
    FileRecord(uint64_t timeoutUs, const std::string& clientVersion,
               const butil::EndPoint& ep)
        : updateTimeUs_(curve::common::TimeUtility::GetTimeofDayUs()),
          timeoutUs_(timeoutUs),
          clientVersion_(clientVersion),
          endPoint_(ep) {}

    FileRecord(const FileRecord& fileRecord)
        : updateTimeUs_(fileRecord.updateTimeUs_),
          timeoutUs_(fileRecord.timeoutUs_),
          clientVersion_(fileRecord.clientVersion_),
          endPoint_(fileRecord.endPoint_) {}

    FileRecord& operator=(const FileRecord& fileRecord) {
        updateTimeUs_ = fileRecord.updateTimeUs_;
        timeoutUs_ = fileRecord.timeoutUs_;
        clientVersion_ = fileRecord.clientVersion_;
        endPoint_ = fileRecord.endPoint_;
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
    void Update(const std::string& clientVersion, const butil::EndPoint& ep) {
        curve::common::LockGuard lk(mtx_);
        updateTimeUs_ = curve::common::TimeUtility::GetTimeofDayUs();
        clientVersion_ = clientVersion;
        endPoint_ = ep;
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

    butil::EndPoint GetClientEndPoint() const {
        return endPoint_;
    }

 private:
    // latest update time in μs
    uint64_t updateTimeUs_;
    // timeout
    uint64_t timeoutUs_;
    // client version
    std::string clientVersion_;
    // client endpoint
    butil::EndPoint endPoint_;
    // mutex for updating the time
    mutable curve::common::Mutex mtx_;
};

class FileRecordManager {
 public:
    virtual ~FileRecordManager() = default;

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
    virtual uint32_t GetFileRecordExpiredTimeUs() const {
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
     * @brief remove file record corresponding to filename and endpoint
     * @param filename file record that to be deleted
     */
    void RemoveFileRecord(const std::string& filename,
                          const std::string& clientIP, uint32_t clientPort);

    /**
     * @brief Get the mininum client version corresponding to the input file
     * @param[in] filename
     */
    bool GetMinimumFileClientVersion(
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

    std::set<butil::EndPoint> ListAllClient() const;

    virtual bool FindFileMountPoint(const std::string& fileName,
                                    std::vector<butil::EndPoint>* eps) const;

 private:
    /**
     * @brief Function for periodic scanning, it deletes timed-out file records
     */
    void Scan();

    // file records
    // There are two scenarios for endpoints of map's key
    // 1. if client enables register to mds, endpoint is corresponding to client
    //    host ip and dummy server port
    // 2. otherwise, ip is equal to rpc's remote_side and port is `kInvalidPort'
    std::unordered_map<std::string, std::map<butil::EndPoint, FileRecord>>
        fileRecords_;
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

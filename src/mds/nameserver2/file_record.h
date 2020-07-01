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
    // file record过期时间，单位us
    uint32_t fileRecordExpiredTimeUs;
    // 后台扫描file record map的时间间隔，单位us
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
     * @brief 是否已经更新超时
     * @return true/超时 false/未超时
     */
    bool IsTimeout() const {
        curve::common::LockGuard lk(mtx_);
        uint64_t currentTimeUs = curve::common::TimeUtility::GetTimeofDayUs();
        return currentTimeUs > updateTimeUs_ + 10 * timeoutUs_;
    }

    /**
     * @brief 更新时间
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
     * @brief 更新版本号
     */
    void Update(const std::string& clientVersion) {
        curve::common::LockGuard lk(mtx_);
        clientVersion_ = clientVersion;
    }

    /**
     * @brief 获取上次更新时间
     * @return 上次更新时间
     */
    uint64_t GetUpdateTime() const {
        curve::common::LockGuard lk(mtx_);
        return updateTimeUs_;
    }

    /**
     * @brief 获取当前打开文件的client版本号
     * @return 版本号, 没有为空
     */
    std::string GetClientVersion() const {
        return clientVersion_;
    }

    ClientIpPortType GetClientIpPort() const {
        return {clientIP_, clientPort_};
    }

 private:
    // 最新更新时间，单位us
    uint64_t updateTimeUs_;
    // 超时时间
    uint64_t timeoutUs_;
    // client版本号
    std::string clientVersion_;
    // client地址
    std::string clientIP_;
    // client端口
    uint32_t clientPort_;
    // 更新时间锁
    mutable curve::common::Mutex mtx_;
};

class FileRecordManager {
 public:
    /**
     * @brief 初始化
     * @param[in] sessionOption session配置项
     */
    void Init(const FileRecordOptions& fileRecordOptions);

    /**
     * @brief 获取当前已打开文件数量
     * @return 当前已打开文件数量
     */
    uint64_t GetOpenFileNum() const {
        ReadLockGuard lk(rwlock_);
        return fileRecords_.size();
    }

    /**
     * @brief 获取文件过期时间
     * @return 文件过期时间
     */
    uint32_t GetFileRecordExpiredTimeUs() const {
        return fileRecordOptions_.fileRecordExpiredTimeUs;
    }

    /**
     * @brief 更新filename对应的文件记录
     * @param[in] filename文件名
     */
    void UpdateFileRecord(const std::string& filename,
                          const std::string& clientVersion,
                          const std::string& clientIP,
                          uint32_t clientPort);

    /**
     * @brief 获取filename对应的client version
     * @param[in] filename文件名
     */
    bool GetFileClientVersion(
        const std::string& fileName, std::string *clientVersion) const;

    /**
     * @brief 启动FileRecordManager
     */
    void Start();

    /**
     * @brief 停止FileRecordManager
     */
    void Stop();

    /**
     * @brief 根据配置项，设置protoSession
     * @param[out] protoSession需要设置的参数
     */
    void GetRecordParam(ProtoSession* protoSession) const;

    std::set<ClientIpPortType> ListAllClient() const;

    bool FindFileMountPoint(const std::string& fileName,
                            ClientIpPortType* ipPort) const;

 private:
    /**
     * @brief 定期扫描函数，删除已经超时的文件记录
     */
    void Scan();

    // 文件记录
    std::unordered_map<std::string, FileRecord> fileRecords_;
    // 保护fileRecords_的读写锁
    mutable curve::common::RWLock rwlock_;
    // 后台扫描执行线程
    curve::common::Thread scanThread_;

    FileRecordOptions fileRecordOptions_;
    curve::common::InterruptibleSleeper sleeper_;
    curve::common::Atomic<bool> running_{false};
};

}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_FILE_RECORD_H_

/**
 * Project : curve
 * Created Date : 2020-03-13
 * Author : wuhanqing
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_FILE_RECORD_H_
#define SRC_MDS_NAMESERVER2_FILE_RECORD_H_

#include <unordered_map>
#include <string>

#include "src/common/concurrent/rw_lock.h"
#include "src/common/interruptible_sleeper.h"
#include "src/common/timeutility.h"
#include "proto/nameserver2.pb.h"

namespace curve {
namespace mds {

using curve::common::ReadLockGuard;
using curve::common::WriteLockGuard;

struct FileRecordOptions {
    // file record过期时间，单位us
    uint32_t fileRecordExpiredTimeUs;
    // 后台扫描file record map的时间间隔，单位us
    uint32_t scanIntervalTimeUs;
};

class FileRecord {
 public:
    explicit FileRecord(uint64_t timeoutUs)
      : updateTimeUs_(curve::common::TimeUtility::GetTimeofDayUs())
      , timeoutUs_(timeoutUs) {}

    FileRecord(const FileRecord& fileRecord)
      : updateTimeUs_(fileRecord.updateTimeUs_)
      , timeoutUs_(fileRecord.timeoutUs_) {}

    FileRecord& operator=(const FileRecord& fileRecord) {
        updateTimeUs_ = fileRecord.updateTimeUs_;
        timeoutUs_ = fileRecord.timeoutUs_;

        return *this;
    }

    /**
     * @brief 是否已经更新超时
     * @return true/超时 false/未超时
     */
    bool IsTimeout() const {
        curve::common::LockGuard lk(mtx_);
        uint64_t currentTimeUs = curve::common::TimeUtility::GetTimeofDayUs();
        return currentTimeUs > updateTimeUs_ + timeoutUs_;
    }

    /**
     * @brief 更新时间
     */
    void Update() {
        curve::common::LockGuard lk(mtx_);
        updateTimeUs_ = curve::common::TimeUtility::GetTimeofDayUs();
    }

    /**
     * @brief 获取上次更新时间
     * @return 上次更新时间
     */
    uint64_t GetUpdateTime() const {
        curve::common::LockGuard lk(mtx_);
        return updateTimeUs_;
    }

 private:
    // 最新更新时间，单位us
    uint64_t updateTimeUs_;
    // 超时时间
    uint64_t timeoutUs_;
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
     * @brief 更新filename对应的文件记录
     * @param[in] filename文件名
     */
    void UpdateFileRecord(const std::string& filename);

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

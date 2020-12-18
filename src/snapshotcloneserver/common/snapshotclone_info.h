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
 * Project: curve
 * Created Date: Wed Jul 01 2020
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_INFO_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_INFO_H_

#include <vector>
#include <string>
#include <map>
#include <memory>

#include "src/common/snapshotclone/snapshotclone_define.h"

namespace curve {
namespace snapshotcloneserver {

enum class CloneStatus {
    done = 0,
    cloning = 1,
    recovering = 2,
    cleaning = 3,
    errorCleaning = 4,
    error = 5,
    retrying = 6,
    metaInstalled = 7,
};

enum class CloneFileType {
    kFile = 0,
    kSnapshot = 1
};

enum class CloneStep {
    kCreateCloneFile = 0,
    kCreateCloneMeta,
    kCreateCloneChunk,
    kCompleteCloneMeta,
    kRecoverChunk,
    kChangeOwner,
    kRenameCloneFile,
    kCompleteCloneFile,
    kEnd
};

// 数据库中clone/recover任务信息
class CloneInfo {
 public:
  CloneInfo()
        : type_(CloneTaskType::kClone),
          originId_(0),
          destinationId_(0),
          time_(0),
          fileType_(CloneFileType::kSnapshot),
          isLazy_(false),
          nextStep_(CloneStep::kCreateCloneFile),
          status_(CloneStatus::error) {}

  CloneInfo(const TaskIdType &taskId,
        const std::string &user,
        CloneTaskType type,
        const std::string &source,
        const std::string &destination,
        CloneFileType fileType,
        bool isLazy)
        : taskId_(taskId),
          user_(user),
          type_(type),
          source_(source),
          destination_(destination),
          originId_(0),
          destinationId_(0),
          time_(0),
          fileType_(fileType),
          isLazy_(isLazy),
          nextStep_(CloneStep::kCreateCloneFile),
          status_(CloneStatus::cloning) {}

  CloneInfo(const TaskIdType &taskId,
        const std::string &user,
        CloneTaskType type,
        const std::string &source,
        const std::string &destination,
        uint64_t originId,
        uint64_t destinationId,
        uint64_t time,
        CloneFileType fileType,
        bool isLazy,
        CloneStep nextStep,
        CloneStatus status)
        : taskId_(taskId),
          user_(user),
          type_(type),
          source_(source),
          destination_(destination),
          originId_(originId),
          destinationId_(destinationId),
          time_(time),
          fileType_(fileType),
          isLazy_(isLazy),
          nextStep_(nextStep),
          status_(status) {}

  TaskIdType GetTaskId() const {
      return taskId_;
  }

  void SetTaskId(const TaskIdType &taskId) {
      taskId_ = taskId;
  }

  std::string GetUser() const {
      return user_;
  }

  void SetUser(const std::string &user) {
      user_ = user;
  }

  CloneTaskType GetTaskType() const {
      return type_;
  }

  void SetTaskType(CloneTaskType type) {
      type_ = type;
  }

  std::string GetSrc() const {
      return source_;
  }

  void SetSrc(const std::string &source) {
      source_ = source;
  }

  std::string GetDest() const {
      return destination_;
  }

  void SetDest(const std::string &dest) {
      destination_ = dest;
  }

  uint64_t GetOriginId() const {
      return originId_;
  }

  void SetOriginId(uint64_t originId) {
      originId_ = originId;
  }

  uint64_t GetDestId() const {
      return destinationId_;
  }

  void SetDestId(uint64_t destId) {
      destinationId_ = destId;
  }

  uint64_t GetTime() const {
      return time_;
  }

  void SetTime(uint64_t time) {
      time_ = time;
  }

  CloneFileType GetFileType() const {
      return fileType_;
  }

  void SetFileType(CloneFileType fileType) {
      fileType_ = fileType;
  }

  bool GetIsLazy() const {
      return isLazy_;
  }

  void SetIsLazy(bool flag) {
      isLazy_ = flag;
  }

  CloneStep GetNextStep() const {
    return nextStep_;
  }

  void SetNextStep(CloneStep nextStep) {
    nextStep_ = nextStep;
  }
  CloneStatus GetStatus() const {
      return status_;
  }

  void SetStatus(CloneStatus status) {
      status_ = status;
  }

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

 private:
    // 任务Id
    TaskIdType  taskId_;
    // 用户
    std::string user_;
    // 克隆或恢复
    CloneTaskType type_;
    // 源文件或快照uuid
    std::string source_;
    // 目标文件名
    std::string destination_;
    // 被恢复的原始文件id, 仅用于恢复
    uint64_t originId_;
    // 目标文件id
    uint64_t destinationId_;
    // 创建时间
    uint64_t time_;
    // 克隆/恢复的文件类型
    CloneFileType fileType_;
    // 是否lazy
    bool isLazy_;
    // 克隆进度, 下一个步骤
    CloneStep nextStep_;
    // 处理的状态
    CloneStatus status_;
};

std::ostream& operator<<(std::ostream& os, const CloneInfo &cloneInfo);

//快照处理状态
enum class Status{
    done = 0,
    pending,
    deleting,
    errorDeleting,
    canceling,
    error
};

//快照信息
class SnapshotInfo {
 public:
    SnapshotInfo()
        :uuid_(),
        seqNum_(kUnInitializeSeqNum),
        chunkSize_(0),
        segmentSize_(0),
        fileLength_(0),
        time_(0),
        status_(Status::pending) {}

    SnapshotInfo(UUID uuid,
            const std::string &user,
            const std::string &fileName,
            const std::string &snapshotName)
        :uuid_(uuid),
        user_(user),
        fileName_(fileName),
        snapshotName_(snapshotName),
        seqNum_(kUnInitializeSeqNum),
        chunkSize_(0),
        segmentSize_(0),
        fileLength_(0),
        time_(0),
        status_(Status::pending) {}
    SnapshotInfo(UUID uuid,
            const std::string &user,
            const std::string &fileName,
            const std::string &desc,
            uint64_t seqnum,
            uint32_t chunksize,
            uint64_t segmentsize,
            uint64_t filelength,
            uint64_t time,
            Status status)
        :uuid_(uuid),
        user_(user),
        fileName_(fileName),
        snapshotName_(desc),
        seqNum_(seqnum),
        chunkSize_(chunksize),
        segmentSize_(segmentsize),
        fileLength_(filelength),
        time_(time),
        status_(status) {}

    void SetUuid(const UUID &uuid) {
        uuid_ = uuid;
    }

    UUID GetUuid() const {
        return uuid_;
    }

    void SetUser(const std::string &user) {
        user_ = user;
    }

    std::string GetUser() const {
        return user_;
    }

    void SetFileName(const std::string &fileName) {
        fileName_ = fileName;
    }

    std::string GetFileName() const {
        return fileName_;
    }

    void SetSnapshotName(const std::string &snapshotName) {
        snapshotName_ = snapshotName;
    }

    std::string GetSnapshotName() const {
        return snapshotName_;
    }

    void SetSeqNum(uint64_t seqNum) {
        seqNum_ = seqNum;
    }

    uint64_t GetSeqNum() const {
        return seqNum_;
    }

    void SetChunkSize(uint32_t chunkSize) {
        chunkSize_ = chunkSize;
    }

    uint32_t GetChunkSize() const {
        return chunkSize_;
    }

    void SetSegmentSize(uint64_t segmentSize) {
        segmentSize_ = segmentSize;
    }

    uint64_t GetSegmentSize() const {
        return segmentSize_;
    }

    void SetFileLength(uint64_t fileLength) {
        fileLength_ = fileLength;
    }

    uint64_t GetFileLength() const {
        return fileLength_;
    }

    void SetCreateTime(uint64_t createTime) {
        time_ = createTime;
    }

    uint64_t GetCreateTime() const {
        return time_;
    }

    void SetStatus(Status status) {
        status_ = status;
    }

    Status GetStatus() const {
        return status_;
    }

    bool SerializeToString(std::string *value) const;

    bool ParseFromString(const std::string &value);

 private:
    // 快照uuid
    UUID uuid_;
    // 租户信息
    std::string user_;
    // 快照目标文件名
    std::string fileName_;
    // 快照名
    std::string snapshotName_;
    // 快照版本号
    uint64_t seqNum_;
    // 文件的chunk大小
    uint32_t chunkSize_;
    // 文件的segment大小
    uint64_t segmentSize_;
    //文件大小
    uint64_t fileLength_;
    // 快照创建时间
    uint64_t time_;
    // 快照处理的状态
    Status status_;
};

std::ostream& operator<<(std::ostream& os, const SnapshotInfo &snapshotInfo);

}  // namespace snapshotcloneserver
}  // namespace curve


#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_INFO_H_

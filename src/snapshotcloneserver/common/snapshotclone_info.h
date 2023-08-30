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

//Clone/recover task information in the database

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
        const std::string &poolset,
        CloneFileType fileType,
        bool isLazy)
        : taskId_(taskId),
          user_(user),
          type_(type),
          source_(source),
          destination_(destination),
          poolset_(poolset),
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
        const std::string &poolset,
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
          poolset_(poolset),
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

  std::string GetPoolset() const {
      return poolset_;
  }

  void SetPoolset(const std::string &poolset) {
      poolset_ = poolset;
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
    //Task Id

    TaskIdType  taskId_;
    //Users

    std::string user_;
    //Clone or Restore

    CloneTaskType type_;
    //Source file or snapshot uuid

    std::string source_;
    //Destination File Name

    std::string destination_;
    //The poolset where the target file is located

    std::string poolset_;
    //The original file ID that has been restored, for recovery purposes only

    uint64_t originId_;
    //Target file id

    uint64_t destinationId_;
    //Creation time

    uint64_t time_;
    //Clone/Restore File Types

    CloneFileType fileType_;
    //Lazy or not

    bool isLazy_;
    //Clone progress, next step

    CloneStep nextStep_;
    //Processing status

    CloneStatus status_;
};

std::ostream& operator<<(std::ostream& os, const CloneInfo &cloneInfo);

//Snapshot processing status

enum class Status{
    done = 0,
    pending,
    deleting,
    errorDeleting,
    canceling,
    error
};

//Snapshot Information

class SnapshotInfo {
 public:
    SnapshotInfo()
        :uuid_(),
        seqNum_(kUnInitializeSeqNum),
        chunkSize_(0),
        segmentSize_(0),
        fileLength_(0),
        stripeUnit_(0),
        stripeCount_(0),
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
        stripeUnit_(0),
        stripeCount_(0),
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
            uint64_t stripeUnit,
            uint64_t stripeCount,
            const std::string& poolset,
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
        stripeUnit_(stripeUnit),
        stripeCount_(stripeCount),
        poolset_(poolset),
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

    void SetStripeUnit(uint64_t stripeUnit) {
        stripeUnit_ = stripeUnit;
    }

    uint64_t GetStripeUnit() const {
        return stripeUnit_;
    }

    void SetStripeCount(uint64_t stripeCount) {
        stripeCount_ = stripeCount;
    }

    uint64_t GetStripeCount() const {
        return stripeCount_;
    }

    void SetPoolset(const std::string& poolset) {
        poolset_ = poolset;
    }

    const std::string& GetPoolset() const {
        return poolset_;
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
    //Snapshot uuid

    UUID uuid_;
    //Tenant Information

    std::string user_;
    //Snapshot Destination File Name

    std::string fileName_;
    //Snapshot Name

    std::string snapshotName_;
    //Snapshot version number

    uint64_t seqNum_;
    //Chunk size of the file

    uint32_t chunkSize_;
    //The segment size of the file

    uint64_t segmentSize_;
    //File size

    uint64_t fileLength_;
    // stripe size
    uint64_t stripeUnit_;
    // stripe count
    uint64_t stripeCount_;
    // poolset
    std::string poolset_;
    //Snapshot creation time

    uint64_t time_;
    //Status of snapshot processing

    Status status_;
};

std::ostream& operator<<(std::ostream& os, const SnapshotInfo &snapshotInfo);

}  // namespace snapshotcloneserver
}  // namespace curve


#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_INFO_H_

/*************************************************************************
> File Name: snapshot_meta_store.h
> Author:
> Created Time: Fri Dec 14 18:25:30 2018
> Copyright (c) 2018 netease
 ************************************************************************/

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_META_STORE_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_META_STORE_H_

#include <vector>
#include <string>
#include <map>
#include <memory>
#include <mutex> //NOLINT

#include "src/snapshotcloneserver/dao/snapshotcloneRepo.h"
#include "src/snapshotcloneserver/common/define.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace snapshotcloneserver {

enum class CloneStatus {
    done = 0,
    cloning = 1,
    recovering = 2,
    cleaning = 3,
    error
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

  CloneInfo(const std::string &taskId,
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

  CloneInfo(const std::string &taskId,
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

  std::string GetTaskId() const {
      return taskId_;
  }

  void SetTaskId(const std::string &taskId) {
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

 private:
    // 任务Id
    std::string  taskId_;
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

    UUID GetUuid() const {
        return uuid_;
    }

    std::string GetUser() const {
        return user_;
    }

    std::string GetFileName() const {
        return fileName_;
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

class SnapshotCloneMetaStore {
 public:
    SnapshotCloneMetaStore() {}
    virtual ~SnapshotCloneMetaStore() {}
    /**
     * 初始化metastore，不同的metastore可以有不同的实现，可以是初始化目录文件或者初始化数据库
     * @return: 0 初始化成功/ 初始化失败 -1
     */
    virtual int Init(const SnapshotCloneMetaStoreOptions &options) = 0;
    // 添加一条快照信息记录
    /**
     * 添加一条快照记录到metastore中
     * @param 快照信息结构体
     * @return: 0 插入成功/ -1 插入失败
     */
    virtual int AddSnapshot(const SnapshotInfo &snapinfo) = 0;
    /**
     * 从metastore删除一条快照记录
     * @param 快照任务的uuid，全局唯一
     * @return 0 删除成功/ -1 删除失败
     */
    virtual int DeleteSnapshot(const UUID &uuid) = 0;
    /**
     * 更新快照记录
     * @param 快照信息结构体
     * @return: 0 更新成功/ -1 更新失败
     */
    virtual int UpdateSnapshot(const SnapshotInfo &snapinfo) = 0;
    /**
     * 获取指定快照的快照信息
     * @param 快照的uuid
     * @param 保存快照信息的指针
     * @return 0 获取成功/ -1 获取失败
     */
    virtual int GetSnapshotInfo(const UUID &uuid, SnapshotInfo *info) = 0;
    /**
     * 获取指定文件的快照信息列表
     * @param 文件名
     * @param 保存快照信息的vector指针
     * @return 0 获取成功/ -1 获取失败
     */
    virtual int GetSnapshotList(const std::string &filename,
                                std::vector<SnapshotInfo> *v) = 0;
    /**
     * 获取全部的快照信息列表
     * @param 保存快照信息的vector指针
     * @return: 0 获取成功/ -1 获取失败
     */
    virtual int GetSnapshotList(std::vector<SnapshotInfo> *list) = 0;
    /**
     * @brief 插入一条clone任务记录到metastore
     * @param clone记录信息
     * @return: 0 插入成功/ -1 插入失败
     */
    virtual int AddCloneInfo(const CloneInfo &cloneInfo) = 0;
    /**
     * @brief 从metastore删除一条clone任务记录
     * @param clone任务的任务id
     * @return: 0 删除成功/ -1 删除失败
     */
    virtual int DeleteCloneInfo(const std::string &taskID) = 0;
    /**
     * @brief 更新一条clone任务记录
     * @param clone记录信息
     * @return: 0 更新成功/ -1 更新失败
     */
    virtual int UpdateCloneInfo(const CloneInfo &cloneInfo) = 0;
    /**
     * @brief 获取指定task id的clone任务信息
     * @param clone任务id
     * @param[out] clone记录信息的指针
     * @return: 0 获取成功/ -1 获取失败
     */
    virtual int GetCloneInfo(const std::string &taskID, CloneInfo *info) = 0;
    /**
     * @brief 获取所有clone任务的信息列表
     * @param[out] 只想clone任务vector指针
     * @return: 0 获取成功/ -1 获取失败
     */
    virtual int GetCloneInfoList(std::vector<CloneInfo> *list) = 0;
};

class DBSnapshotCloneMetaStore : public SnapshotCloneMetaStore{
 public:
    explicit DBSnapshotCloneMetaStore(
        std::shared_ptr<curve::snapshotcloneserver::SnapshotCloneRepo> repo)
        :repo_(repo) {}
    ~DBSnapshotCloneMetaStore() {}
    int Init(const SnapshotCloneMetaStoreOptions &options) override;
    int AddSnapshot(const SnapshotInfo &snapinfo) override;
    int DeleteSnapshot(const UUID &uuid) override;
    int UpdateSnapshot(const SnapshotInfo &snapinfo) override;
    int GetSnapshotInfo(const UUID &uuid, SnapshotInfo *info) override;
    int GetSnapshotList(const std::string &filename,
                        std::vector<SnapshotInfo> *v) override;
    int GetSnapshotList(std::vector<SnapshotInfo> *list) override;

    int AddCloneInfo(const CloneInfo &cloneInfo) override;

    int DeleteCloneInfo(const std::string &taskID) override;

    int UpdateCloneInfo(const CloneInfo &cloneInfo) override;

    int GetCloneInfo(const std::string &taskID, CloneInfo *info) override;

    int GetCloneInfoList(std::vector<CloneInfo> *list) override;

 private:
    /**
     * 启动时从metastore加载快照任务信息到内存中
     * @return: 0 加载成功/ -1 加载失败
     */
    int LoadSnapshotInfos();
    /**
     * @brief 启动时从metastore加载clone任务的信息到内存中
     * @return: 0 加载成功/ -1 加载失败
     */
    int LoadCloneInfos();

    // db metastore的repo实现
    std::shared_ptr<curve::snapshotcloneserver::SnapshotCloneRepo> repo_;
    // key is UUID, map 需要考虑并发保护
    std::map<UUID, SnapshotInfo> snapInfos_;
    // 考虑使用rwlock
    std::mutex snapInfos_mutex;
    // key is TaskIdType, map 需要考虑并发保护
    std::map<std::string, CloneInfo> cloneInfos_;
    // clone info map lock
    curve::common::RWLock cloneInfos_lock_;
};
}  // namespace snapshotcloneserver
}  // namespace curve
#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_SNAPSHOTCLONE_META_STORE_H_

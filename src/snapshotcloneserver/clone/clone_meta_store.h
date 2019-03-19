/*
 * Project: curve
 * Created Date: Thu Mar 21 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_META_STORE_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_META_STORE_H_

#include <vector>
#include <string>
#include <map>
#include <mutex> //NOLINT
#include "src/snapshotcloneserver/dao/snapshotRepo.h"
#include "src/snapshotcloneserver/common/define.h"
#include "src/common/concurrent/rw_lock.h"

namespace curve {
namespace snapshotcloneserver {

enum class CloneStatus {
    done = 0,
    cloning,
    recovering,
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
struct CloneInfo {
    CloneInfo()
        : type(CloneTaskType::kClone),
          originId(0),
          destinationId(0),
          time(0),
          fileType(CloneFileType::kSnapshot),
          isLazy(false),
          nextStep(CloneStep::kCreateCloneFile),
          status(CloneStatus::error) {}

    CloneInfo(const TaskIdType &taskId,
        const std::string &user,
        CloneTaskType type,
        const UUID &source,
        const std::string &destination,
        CloneFileType fileType,
        bool isLazy)
        : taskId(taskId),
          user(user),
          type(type),
          source(source),
          destination(destination),
          originId(0),
          destinationId(0),
          time(0),
          fileType(fileType),
          isLazy(isLazy),
          nextStep(CloneStep::kCreateCloneFile),
          status(CloneStatus::cloning) {}

    // 任务Id
    TaskIdType taskId;
    // 用户
    std::string user;
    // 克隆或恢复
    CloneTaskType type;
    // 源文件或快照uuid
    UUID source;
    // 目标文件名
    std::string destination;
    // 克隆或恢复时新建文件的inode id
    uint64_t originId;
    // 目标文件inode id, 克隆场景下与originId相同
    uint64_t destinationId;
    // 创建时间
    uint64_t time;
    // 克隆/恢复的文件类型
    CloneFileType fileType;
    // 是否lazy
    bool isLazy;
    // 克隆进度, 下一个步骤
    CloneStep nextStep;
    // 处理的状态
    CloneStatus status;
};

class CloneMetaStore {
 public:
    CloneMetaStore() {}
    virtual ~CloneMetaStore() {}


    virtual int Init() = 0;

    virtual int AddCloneInfo(const CloneInfo &cloneInfo) = 0;

    virtual int DeleteCloneInfo(const TaskIdType &uuid) = 0;

    virtual int UpdateCloneInfo(const CloneInfo &cloneInfo) = 0;

    virtual int GetCloneInfo(const TaskIdType &uuid, CloneInfo *info) = 0;

    virtual int GetCloneInfoList(std::vector<CloneInfo> *list) = 0;
};

class DBCloneMetaStore : public CloneMetaStore {
 public:
    explicit DBCloneMetaStore(std::shared_ptr<RepoInterface> repo)
        :repo_(repo) {}

    int Init() override {return -1;}

    int AddCloneInfo(const CloneInfo &cloneInfo) override {return -1;}

    int DeleteCloneInfo(const TaskIdType &uuid) override {return -1;}

    int UpdateCloneInfo(const CloneInfo &cloneInfo) override {return -1;}

    int GetCloneInfo(const TaskIdType &uuid,
        CloneInfo *info) override {return -1;}

    int GetCloneInfoList(std::vector<CloneInfo> *list) override {return -1;}

 private:
    int LoadCloneInfos();

    // db metaStore的repo实现，与DBSnapshotMetaStore共用
    std::shared_ptr<RepoInterface> repo_;
    // key is TaskIdType, map 需要考虑并发保护
    // TODO(zhaojianming): 考虑后续移除这个map，直接从数据库获取数据
    std::map<TaskIdType, CloneInfo> cloneInfos_;
    // 锁
    curve::common::RWLock cloneInfos_lock;
};





}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_META_STORE_H_

/*
 * Project: curve
 * Created Date: Monday November 26th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_NAMESERVER2_CLEAN_MANAGER_H_
#define SRC_MDS_NAMESERVER2_CLEAN_MANAGER_H_

#include "proto/nameserver2.pb.h"
#include "src/mds/nameserver2/clean_task_manager.h"
#include "src/mds/nameserver2/clean_core.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/nameserver2/async_delete_snapshot_entity.h"

namespace  curve {
namespace mds {

class CleanManagerInterface {
 public:
    virtual ~CleanManagerInterface() {}
    virtual bool SubmitDeleteSnapShotFileJob(const FileInfo&,
      std::shared_ptr<AsyncDeleteSnapShotEntity> entity) = 0;
    virtual std::shared_ptr<Task> GetTask(TaskIDType id) = 0;
    virtual bool SubmitDeleteCommonFileJob(const FileInfo&) = 0;
};
/**
 * CleanManager 用于异步清理 删除快照对应的数据
 * 1. 接收在线的删除快照请求
 * 2. 线程池异步处理实际的chunk删除任务
 **/
class CleanManager : public CleanManagerInterface {
 public:
    explicit CleanManager(std::shared_ptr<CleanCore> core,
                std::shared_ptr<CleanTaskManager> taskMgr,
                NameServerStorage *storage);

    ~CleanManager() {}

    bool Start(void);

    bool Stop(void);

    bool SubmitDeleteSnapShotFileJob(const FileInfo &fileInfo,
         std::shared_ptr<AsyncDeleteSnapShotEntity> entity) override;

    bool SubmitDeleteCommonFileJob(const FileInfo&fileInfo) override;

    bool RecoverCleanTasks(void);

    std::shared_ptr<Task> GetTask(TaskIDType id) override;

 private:
    // TODO(hzsunjianliang): change to std::shared_ptr
    NameServerStorage *storage_;
    std::shared_ptr<CleanCore> cleanCore_;
    std::shared_ptr<CleanTaskManager> taskMgr_;
};

}  // namespace mds
}  // namespace curve
#endif  // SRC_MDS_NAMESERVER2_CLEAN_MANAGER_H_

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
 * Created Date: Monday November 26th 2018
 * Author: hzsunjianliang
 */

#ifndef SRC_MDS_NAMESERVER2_CLEAN_MANAGER_H_
#define SRC_MDS_NAMESERVER2_CLEAN_MANAGER_H_

#include <memory>
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
                std::shared_ptr<NameServerStorage> storage);

    bool Start(void);

    bool Stop(void);

    bool SubmitDeleteSnapShotFileJob(const FileInfo &fileInfo,
         std::shared_ptr<AsyncDeleteSnapShotEntity> entity) override;

    bool SubmitDeleteCommonFileJob(const FileInfo&fileInfo) override;

    bool RecoverCleanTasks(void);

    std::shared_ptr<Task> GetTask(TaskIDType id) override;

 private:
    std::shared_ptr<NameServerStorage> storage_;
    std::shared_ptr<CleanCore> cleanCore_;
    std::shared_ptr<CleanTaskManager> taskMgr_;
};

}  // namespace mds
}  // namespace curve
#endif  // SRC_MDS_NAMESERVER2_CLEAN_MANAGER_H_

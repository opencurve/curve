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
 * Created Date: 2023-07-20
 * Author: xuchaojie
 */

#ifndef SRC_MDS_NAMESERVER2_FLATTEN_MANAGER_H_
#define SRC_MDS_NAMESERVER2_FLATTEN_MANAGER_H_

#include <map>
#include <string>
#include <memory>

#include "proto/nameserver2.pb.h"
#include "src/mds/nameserver2/task_manager.h"
#include "src/mds/nameserver2/flatten_core.h"

namespace curve {
namespace mds {

class FlattenTask : public CancelableTask {
 public:
    FlattenTask(uint64_t taskId,
                const std::string &fileName,
                const FileInfo &fileInfo,
                const FileInfo &snapFileInfo,
                const std::shared_ptr<FlattenCore> &flattenCore)
        : fileName_(fileName),
          fileInfo_(fileInfo),
          snapFileInfo_(snapFileInfo),
          flattenCore_(flattenCore) {
            SetTaskID(taskId);
            SetTaskProgress(TaskProgress());
            SetRetryTimes(kDefaultTaskRetryTimes);
        }
    virtual ~FlattenTask() = default;

    void Run() override;

 private:
    std::string fileName_;
    FileInfo fileInfo_;
    FileInfo snapFileInfo_;
    std::shared_ptr<FlattenCore> flattenCore_;
};

class FlattenManager {
 public:
    FlattenManager() = default;
    virtual ~FlattenManager() = default;

    virtual bool SubmitFlattenJob(uint64_t uniqueId,
                                  const std::string &fileName,
                                  const FileInfo &fileInfo,
                                  const FileInfo &snapFileInfo) = 0;

    virtual std::shared_ptr<FlattenTask> GetFlattenTask(uint64_t uniqueId) = 0;
};

class FlattenManagerImpl : public FlattenManager {
 public:
    FlattenManagerImpl(const std::shared_ptr<FlattenCore> &flattenCore,
                       const std::shared_ptr<TaskManager> &taskManager)
        : flattenCore_(flattenCore),
          taskManager_(taskManager) {}

    virtual ~FlattenManagerImpl() = default;

    bool Start(void);

    bool Stop(void);

    bool SubmitFlattenJob(uint64_t uniqueId,
                          const std::string &fileName,
                          const FileInfo &fileInfo,
                          const FileInfo &snapFileInfo) override;

    std::shared_ptr<FlattenTask> GetFlattenTask(uint64_t uniqueId) override;

 private:
    std::shared_ptr<FlattenCore> flattenCore_;
    std::shared_ptr<TaskManager> taskManager_;
};

}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_NAMESERVER2_FLATTEN_MANAGER_H_

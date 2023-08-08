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


#include "src/mds/nameserver2/flatten_manager.h"


namespace curve {
namespace mds {

void FlattenTask::Run() {
    // do flatten
    flattenCore_->DoFlatten(
        fileName_, fileInfo_, 
        virtualFileInfo_,
        GetMutableTaskProgress());
    return;
}

bool FlattenManagerImpl::Start(void) {
    return taskManager_->Start();
}

bool FlattenManagerImpl::Stop(void) {
    return taskManager_->Stop();
}

bool FlattenManagerImpl::SubmitFlattenJob(uint64_t uniqueId,
                      const std::string &fileName,
                      const FileInfo &fileInfo, 
                      const FileInfo &virtualFileInfo) {
    // submit task
    std::shared_ptr<FlattenTask> task = std::make_shared<FlattenTask>(
        uniqueId, fileName, fileInfo, virtualFileInfo, flattenCore_);

    return taskManager_->SubmitTask(task);
}

std::shared_ptr<FlattenTask> 
FlattenManagerImpl::GetFlattenTask(uint64_t uniqueId) {
    auto task =  taskManager_->GetTask(uniqueId);
    return std::dynamic_pointer_cast<FlattenTask>(task);
}

}  // namespace mds
}  // namespace curve


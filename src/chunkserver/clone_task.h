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
 * Created Date: Monday March 11th 2019
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_CLONE_TASK_H_
#define SRC_CHUNKSERVER_CLONE_TASK_H_

#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <memory>
#include <string>

#include "include/chunkserver/chunkserver_common.h"
#include "src/common/uncopyable.h"
#include "src/chunkserver/clone_copyer.h"
#include "src/chunkserver/clone_core.h"

namespace curve {
namespace chunkserver {

using curve::common::Uncopyable;

class CloneTask : public Uncopyable
                , public std::enable_shared_from_this<CloneTask>{
 public:
    CloneTask(std::shared_ptr<ReadChunkRequest> request,
              std::shared_ptr<CloneCore> core,
              ::google::protobuf::Closure* done)
        : core_(core)
        , readRequest_(request)
        , done_(done)
        , isComplete_(false) {}

    virtual ~CloneTask() {}

    virtual std::function<void()> Closure() {
        auto sharedThis = shared_from_this();
        return [sharedThis] () {
            sharedThis->Run();
        };
    }

    virtual void Run() {
        if (core_ != nullptr) {
            core_->HandleReadRequest(readRequest_, done_);
        }
        isComplete_ = true;
    }

    virtual bool IsComplete() {
        return isComplete_;
    }

 protected:
    // Clone core logic
    std::shared_ptr<CloneCore> core_;
    // Information about this task
    std::shared_ptr<ReadChunkRequest> readRequest_;
    // Closure to be executed after the task
    ::google::protobuf::Closure* done_;
    // Whether the mission is over
    bool isComplete_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CLONE_TASK_H_

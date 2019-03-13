/*
 * Project: curve
 * Created Date: Monday March 11th 2019
 * Author: yangyaokai
 * Copyright (c) 2018 netease
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
    // 克隆核心逻辑
    std::shared_ptr<CloneCore> core_;
    // 此次任务相关信息
    std::shared_ptr<ReadChunkRequest> readRequest_;
    // 任务结束后要执行的Closure
    ::google::protobuf::Closure* done_;
    // 任务是否结束
    bool isComplete_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CLONE_TASK_H_

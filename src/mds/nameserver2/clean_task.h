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
 * Created Date: Tuesday December 18th 2018
 * Author: hzsunjianliang
 */

#ifndef SRC_MDS_NAMESERVER2_CLEAN_TASK_H_
#define SRC_MDS_NAMESERVER2_CLEAN_TASK_H_

#include <functional>
#include <memory>  //NOLINT
#include <string>
#include <brpc/closure_guard.h>  //NOLINT
#include <brpc/controller.h>    //NOLINT
#include "proto/nameserver2.pb.h"
#include "src/mds/nameserver2/task_progress.h"
#include "src/mds/nameserver2/clean_core.h"
#include "src/mds/nameserver2/async_delete_snapshot_entity.h"
#include "src/common/concurrent/dlock.h"
#include "src/common/concurrent/count_down_event.h"

using curve::common::DLock;

namespace curve {
namespace mds {

typedef  uint64_t TaskIDType;

// default clean task retry times
const uint32_t kDefaultTaskRetryTimes = 5;

class Task {
 public:
    Task() : taskID_(0), progress_(), retry_(1) {}

    virtual void Run(void) = 0;

    std::function<void()> Closure() {
        return [this] () {
            Run();
        };
    }

    TaskProgress GetTaskProgress(void) const {
        return progress_;
    }

    void SetTaskProgress(TaskProgress progress) {
        progress_ = progress;
    }

    TaskProgress* GetMutableTaskProgress(void) {
        return &progress_;
    }

    void SetTaskID(TaskIDType taskID) {
        taskID_ = taskID;
    }

    TaskIDType GetTaskID(void) const {
        return taskID_;
    }

    void SetRetryTimes(uint32_t retry) {
        retry_ = retry;
    }

    void Retry() {
        retry_--;
        progress_ = TaskProgress();
    }

    bool RetryTimesExceed() {
        return retry_ == 0;
    }

 protected:
    TaskIDType taskID_;
    TaskProgress progress_;
    // 任务最大重试次数
    uint32_t retry_;
};

class SnapShotCleanTask: public Task {
 public:
    SnapShotCleanTask(TaskIDType taskID, std::shared_ptr<CleanCore> core,
                FileInfo fileInfo,
                std::shared_ptr<AsyncDeleteSnapShotEntity> entity = nullptr) {
        cleanCore_ = core;
        fileInfo_ = fileInfo;
        SetTaskProgress(TaskProgress());
        SetTaskID(taskID);
        asyncEntity_ = entity;
        SetRetryTimes(kDefaultTaskRetryTimes);
    }
    void Run(void) override {
        StatusCode ret = cleanCore_->CleanSnapShotFile(fileInfo_,
                                                    GetMutableTaskProgress());
        if (asyncEntity_ != nullptr) {
            brpc::ClosureGuard doneGuard(asyncEntity_->GetClosure());
            brpc::Controller* cntl =
                static_cast<brpc::Controller*>(asyncEntity_->GetController());
            DeleteSnapShotResponse *response =
                        asyncEntity_->GetDeleteResponse();
            const DeleteSnapShotRequest  *request  =
                        asyncEntity_->GetDeleteRequest();

            response->set_statuscode(ret);
            if (ret != StatusCode::kOK) {
                LOG(ERROR) << "logid = " << cntl->log_id()
                    << ", CleanSnapShotFile fail, filename = "
                    << request->filename()
                    << ", sequencenum = " << request->seq()
                    << ", statusCode = " << ret;
            } else {
                LOG(INFO) << "logid = " << cntl->log_id()
                    << ", CleanSnapShotFile ok, filename = "
                    <<  request->filename()
                    << ", sequencenum = " << request->seq();
            }
        }
        return;
    }

 private:
    std::shared_ptr<CleanCore> cleanCore_;
    FileInfo fileInfo_;
    std::shared_ptr<AsyncDeleteSnapShotEntity> asyncEntity_;
};

class CommonFileCleanTask: public Task {
 public:
    CommonFileCleanTask(TaskIDType taskID, std::shared_ptr<CleanCore> core,
                FileInfo fileInfo) {
        cleanCore_ = core;
        fileInfo_ = fileInfo;
        SetTaskProgress(TaskProgress());
        SetTaskID(taskID);
        SetRetryTimes(kDefaultTaskRetryTimes);
    }

    void Run(void) override {
        cleanCore_->CleanFile(fileInfo_, GetMutableTaskProgress());
        return;
    }

 private:
    std::shared_ptr<CleanCore> cleanCore_;
    FileInfo fileInfo_;
};

class SegmentCleanTask : public Task {
 public:
    SegmentCleanTask(std::shared_ptr<CleanCore> cleanCore,
                     const std::string& cleanSegmentKey,
                     const DiscardSegmentInfo& discardSegmentInfo,
                     curve::common::CountDownEvent* counter,
                     std::shared_ptr<DLock> dlock)
        : Task(),
          cleanCore_(cleanCore),
          cleanSegmentKey_(cleanSegmentKey),
          discardSegmentInfo_(discardSegmentInfo),
          counter_(counter),
          dlock_(dlock) {}

    void Run() override {
        auto finish = [](curve::common::CountDownEvent* counter) {
            counter->Signal();
        };

        // regardless of success or failure, mark this job finished in the end
        std::unique_ptr<curve::common::CountDownEvent, decltype(finish)> guard(
            counter_, finish);

        if (EtcdErrCode::EtcdOK != dlock_->Lock()) {
            LOG(ERROR) << "Get dlock failed in SegmentCleanTask, "
                       << "dlock key is " << dlock_->GetPrefix();
            return;
        }
        cleanCore_->CleanDiscardSegment(cleanSegmentKey_, discardSegmentInfo_,
                                        GetMutableTaskProgress());
        if (nullptr != dlock_) {
            dlock_->Unlock();
        }
        return;
    }

 private:
    std::shared_ptr<CleanCore> cleanCore_;
    std::string cleanSegmentKey_;
    DiscardSegmentInfo discardSegmentInfo_;
    curve::common::CountDownEvent* counter_;
    std::shared_ptr<DLock> dlock_;
};

}  // namespace mds
}  // namespace curve
#endif      //  SRC_MDS_NAMESERVER2_CLEAN_TASK_H_

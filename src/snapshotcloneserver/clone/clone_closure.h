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
 * Created Date: Mon Sep 02 2019
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_CLOSURE_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_CLOSURE_H_


#include <brpc/server.h>
#include <string>
#include <memory>

#include "proto/snapshotcloneserver.pb.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "json/json.h"
#include "src/common/concurrent/name_lock.h"
#include "src/common/concurrent/dlock.h"

using ::curve::common::DLock;
using ::curve::common::NameLockGuard;
using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;

namespace curve {
namespace snapshotcloneserver {

class CloneClosure : public Closure {
 public:
    explicit CloneClosure(brpc::Controller *bcntl = nullptr,
                          Closure *done = nullptr)
        : dlock_(nullptr), bcntl_(bcntl), done_(done), requestId_(""),
          taskId_(""), retCode_(kErrCodeInternalError) {}

    brpc::Controller *GetController() { return bcntl_; }

    void SetRequestId(const UUID &requestId) { requestId_ = requestId; }

    void SetTaskId(const TaskIdType &taskId) { taskId_ = taskId; }

    TaskIdType GetTaskId() { return taskId_; }

    void SetErrCode(int retCode) { retCode_ = retCode; }

    int GetErrCode() { return retCode_; }

    void SetDestFileLock(std::shared_ptr<NameLock> lock) { lock_ = lock; }

    void SetDLock(std::shared_ptr<DLock> lock) { dlock_ = lock; }

    std::shared_ptr<DLock> GetDLock() { return dlock_; }

    void SetDestFileName(const std::string &destFileName) {
        destFileName_ = destFileName;
    }

    void Run() {
        if (done_ != nullptr && bcntl_ != nullptr) {
            brpc::ClosureGuard done_guard(done_);
            if (retCode_ < 0) {
                bcntl_->http_response().set_status_code(
                    brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
                butil::IOBufBuilder os;
                std::string msg =
                    BuildErrorMessage(retCode_, requestId_, taskId_);
                os << msg;
                os.move_to(bcntl_->response_attachment());
            } else {
                bcntl_->http_response().set_status_code(brpc::HTTP_STATUS_OK);
                butil::IOBufBuilder os;
                Json::Value mainObj;
                mainObj["Code"] = std::to_string(kErrCodeSuccess);
                mainObj["Message"] = code2Msg[kErrCodeSuccess];
                mainObj["RequestId"] = requestId_;
                mainObj["UUID"] = taskId_;
                os << mainObj.toStyledString();
                os.move_to(bcntl_->response_attachment());
            }
            LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                      << "action = Clone/Recover"
                      << ", requestId = " << requestId_
                      << ", context = " << bcntl_->response_attachment();
            done_ = nullptr;
            bcntl_ = nullptr;
        }
        if (lock_ != nullptr) {
            lock_->Unlock(destFileName_);
        }

        if (dlock_ != nullptr) {
            dlock_->Unlock();
        }
    }

 private:
    std::shared_ptr<NameLock> lock_;
    std::shared_ptr<DLock> dlock_;
    std::string destFileName_;
    brpc::Controller *bcntl_;
    Closure *done_;
    UUID requestId_;
    TaskIdType taskId_;
    int retCode_;
};

}  // namespace snapshotcloneserver
}  // namespace curve


#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_CLOSURE_H_

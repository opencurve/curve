/*
 * Project: curve
 * Created Date: Mon Sep 02 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_CLOSURE_H_
#define SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_CLOSURE_H_


#include <brpc/server.h>
#include <string>
#include <memory>

#include "proto/snapshotcloneserver.pb.h"
#include "src/snapshotcloneserver/common/define.h"
#include "json/json.h"
#include "src/common/concurrent/name_lock.h"

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::curve::common::NameLockGuard;

namespace curve {
namespace snapshotcloneserver {

class CloneClosure : public Closure {
 public:
    CloneClosure(brpc::Controller* bcntl = nullptr,
                 Closure* done = nullptr)
        : bcntl_(bcntl),
          done_(done),
          requestId_(""),
          taskId_(""),
          retCode_(kErrCodeInternalError) {}

    brpc::Controller * GetController() {
        return bcntl_;
    }

    void SetRequestId(const UUID &requestId) {
        requestId_ = requestId;
    }

    void SetTaskId(const TaskIdType &taskId) {
        taskId_ = taskId;
    }

    TaskIdType GetTaskId() {
        return taskId_;
    }

    void SetErrCode(int retCode) {
        retCode_ = retCode;
    }

    int GetErrCode() {
        return retCode_;
    }

    void SetDestFileLock(std::shared_ptr<NameLock> lock) {
        lock_ = lock;
    }

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
                std::string msg = BuildErrorMessage(retCode_,
                                                    requestId_,
                                                    taskId_);
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
    }

 private:
    std::shared_ptr<NameLock> lock_;
    std::string destFileName_;
    brpc::Controller *bcntl_;
    Closure* done_;
    UUID requestId_;
    TaskIdType taskId_;
    int retCode_;
};

}  // namespace snapshotcloneserver
}  // namespace curve


#endif  // SRC_SNAPSHOTCLONESERVER_CLONE_CLONE_CLOSURE_H_

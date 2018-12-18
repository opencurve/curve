/*
 * Project: curve
 * Created Date: 18-10-13
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CLIENT_MOCK_REQUEST_CONTEXT_H
#define CURVE_CLIENT_MOCK_REQUEST_CONTEXT_H

#include "src/client/client_common.h"
#include "src/client/request_context.h"
#include "src/client/request_closure.h"

namespace curve {
namespace client {

class CountDownEvent {
 public:
    explicit CountDownEvent(int initCnt) :
        mutex_(),
        cond_(),
        count_(initCnt) {
    }

    void Signal() {
        std::unique_lock<std::mutex> guard(mutex_);
        --count_;
        if (count_ <= 0) {
            cond_.notify_all();
        }
    }

    void Wait() {
        std::unique_lock<std::mutex> guard(mutex_);
        while (count_ > 0) {
            cond_.wait(guard);
        }
    }

 private:
    mutable std::mutex mutex_;
    std::condition_variable cond_;
    int count_;
};

class FakeRequestContext : public RequestContext {
 public:
    FakeRequestContext() : RequestContext(nullptr) {}
    virtual ~FakeRequestContext() {}
};

class FakeRequestClosure : public RequestClosure {
 public:
    explicit FakeRequestClosure(CountDownEvent *cond, RequestContext *reqctx)
        : cond_(cond),
          RequestClosure(reqctx) {
        reqCtx_ = reqctx;
    }
    virtual ~FakeRequestClosure() {}

    void Run() override {
        if (0 == errcode_) {
            LOG(INFO) << "success";
        } else {
            LOG(INFO) << "errno: " << errcode_;
        }
        if (nullptr != cond_) {
            cond_->Signal();
        }
    }
    void SetFailed(int err) override {
        errcode_ = err;
    }
    int GetErrorCode() override {
        return errcode_;
    }
    RequestContext *GetReqCtx() override {
        return reqCtx_;
    }

 private:
    CountDownEvent *cond_;

 private:
    int errcode_ = -1;
    IOContext *ctx_;
    RequestContext *reqCtx_;
};

}   // namespace client
}   // namespace curve

#endif  // CURVE_CLIENT_MOCK_REQUEST_CONTEXT_H

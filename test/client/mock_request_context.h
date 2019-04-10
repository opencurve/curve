/*
 * Project: curve
 * Created Date: 18-10-13
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef TEST_CLIENT_MOCK_REQUEST_CONTEXT_H_
#define TEST_CLIENT_MOCK_REQUEST_CONTEXT_H_

#include "src/client/client_common.h"
#include "src/client/request_context.h"
#include "src/client/request_closure.h"
#include "src/common/concurrent/count_down_event.h"

namespace curve {
namespace client {

class FakeRequestContext : public RequestContext {
 public:
    FakeRequestContext() : RequestContext() {}
    virtual ~FakeRequestContext() {}
};

class FakeRequestClosure : public RequestClosure {
 public:
    explicit FakeRequestClosure(curve::common::CountDownEvent *cond,
                                RequestContext *reqctx)
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

    IOTracker* GettIOTracker() {
        return tracker_;
    }

 private:
    curve::common::CountDownEvent *cond_;

 private:
    int errcode_ = -1;
    IOTracker *tracker_;
    RequestContext *reqCtx_;
};

}   // namespace client
}   // namespace curve

#endif  // TEST_CLIENT_MOCK_REQUEST_CONTEXT_H_

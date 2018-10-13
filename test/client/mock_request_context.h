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

class FakeRequestContext : public RequestContext {
 public:
    FakeRequestContext() : RequestContext(nullptr) {}
    virtual ~FakeRequestContext() {}
};

class FakeRequestClosure : public RequestClosure {
 public:
    explicit FakeRequestClosure(RequestContext *reqctx)
        : RequestClosure(reqctx) {
        reqCtx_ = reqctx;
    }
    virtual ~FakeRequestClosure() {}

    void Run() override {
        if (0 == errcode_) {
        } else {
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
    int         errcode_ = -1;
    IOContext   *ctx_;
    RequestContext *reqCtx_;
};

}   // namespace client
}   // namespace curve

#endif  // CURVE_CLIENT_MOCK_REQUEST_CONTEXT_H

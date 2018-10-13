/*
 * Project: curve
 * File Created: Monday, 8th October 2018 4:47:17 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_LIBCURVE_REQUEST_CLOSURE_H
#define CURVE_LIBCURVE_REQUEST_CLOSURE_H

// for Closure
#include <google/protobuf/stubs/callback.h>
#include "include/curve_compiler_specific.h"

namespace curve {
namespace client {

class IOContext;
class RequestContext;
class RequestClosure : public ::google::protobuf::Closure {
 public:
    explicit RequestClosure(RequestContext* reqctx);
    CURVE_MOCK ~RequestClosure();

    void SetIOContext(IOContext* ioctx);
    CURVE_MOCK void SetFailed(int errorcode);
    CURVE_MOCK void Run();
    CURVE_MOCK int GetErrorCode();
    CURVE_MOCK RequestContext* GetReqCtx();
    void Reset();

 private:
    int  errcode_;
    IOContext* ctx_;
    RequestContext* reqCtx_;
};
}   // namespace client
}   // namespace curve

#endif  // !CURVE_LIBCURVE_REQUEST_CLOSURE_H

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
class IOTracker;
class RequestContext;

class RequestClosure : public ::google::protobuf::Closure {
 public:
    explicit RequestClosure(RequestContext* reqctx);
    virtual ~RequestClosure() = default;

    /**
     * clouser的callback执行函数
     */
    virtual void Run();

    /**
     *  获取request的错误码
     */
    virtual int GetErrorCode();

    /**
     *  获取当前closure属于哪个request
     */
    virtual RequestContext* GetReqCtx();

    /**
     * 获取当前request属于哪个iotracker
     */
    virtual IOTracker* GetIOTracker();

    /**
     * 设置返回错误
     */
    virtual void SetFailed(int errorcode);

    /**
     * 设置当前属于哪一个iotracker
     */
    void SetIOTracker(IOTracker* ioctx);

 private:
    // 当前request的错误码
    int  errcode_;

    // 当前request的tracker信息
    IOTracker* tracker_;

    // closure的request信息
    RequestContext* reqCtx_;
};
}   // namespace client
}   // namespace curve

#endif  // !CURVE_LIBCURVE_REQUEST_CLOSURE_H

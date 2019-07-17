/*
 * Project: curve
 * File Created: Monday, 8th October 2018 4:47:17 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef SRC_CLIENT_REQUEST_CLOSURE_H_
#define SRC_CLIENT_REQUEST_CLOSURE_H_

// for Closure
#include <google/protobuf/stubs/callback.h>
#include "include/curve_compiler_specific.h"
#include "src/client/client_metric.h"

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

    /**
     * 设置当前closure重试次数
     */
    void IncremRetriedTimes() {
       retryTimes_++;
    }

    uint64_t GetRetriedTimes() {
       return retryTimes_;
    }

    /**
     * 设置metric
     */
    void SetFileMetric(FileMetric_t* fm);

    /**
     * 获取metric指针
     */
    FileMetric_t* GetMetric();

    /**
     * 设置rpc发送起始时间，用于RPC延时统计
     */
    void SetStartTime(uint64_t start);

    /**
     * 获取rpc起始时间
     */
    uint64_t GetStartTime();

 private:
    // 当前request的错误码
    int  errcode_;

    // 当前request的tracker信息
    IOTracker* tracker_;

    // closure的request信息
    RequestContext* reqCtx_;

    // metric信息
    FileMetric_t* metric_;

    // 起始时间
    uint64_t starttime_;

    // 重试次数
    uint64_t retryTimes_;
};
}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_REQUEST_CLOSURE_H_

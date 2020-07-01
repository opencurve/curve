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
 * File Created: Monday, 8th October 2018 4:47:17 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_REQUEST_CLOSURE_H_
#define SRC_CLIENT_REQUEST_CLOSURE_H_

// for Closure
#include <google/protobuf/stubs/callback.h>
#include <iostream>
#include <map>

#include "include/curve_compiler_specific.h"
#include "src/client/inflight_controller.h"
#include "src/client/client_metric.h"
#include "src/client/config_info.h"
#include "src/client/client_common.h"
#include "src/common/concurrent/concurrent.h"

using curve::common::RWLock;

namespace curve {
namespace client {
class IOTracker;
class RequestContext;
class IOManager;

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
     * @brief 设置所属的iomanager
     */
    void SetIOManager(IOManager* ioManager);

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
    void SetFileMetric(FileMetric* fm);

    /**
     * 获取metric指针
     */
    FileMetric* GetMetric();

    /**
     * 设置rpc发送起始时间，用于RPC延时统计
     */
    void SetStartTime(uint64_t start);

    /**
     * 获取rpc起始时间
     */
    uint64_t GetStartTime();

    /**
     * 发送rpc之前都需要先获取inflight token
     */
    void GetInflightRPCToken();

    /**
     * 返回给用户或者重新进队的时候都要释放inflight token
     */
    void ReleaseInflightRPCToken();

    /**
     * 获取下一次rpc超时时间, rpc超时时间实现了指数退避的策略
     */
    uint64_t GetNextTimeoutMS() {
       return nextTimeoutMS_;
    }

    /**
     * 设置下次重试超时时间
     */
    void SetNextTimeOutMS(uint64_t timeout) {
       nextTimeoutMS_ = timeout;
    }

    /**
     * 设置当前的IO为悬挂IO
     */
    void SetSuspendRPCFlag() {
       suspendRPC_ = true;
    }

    bool IsSuspendRPC() {
       return suspendRPC_;
    }

 private:
    // suspend io标志
    bool suspendRPC_;

    // 当前request的错误码
    int  errcode_;

    // 当前request的tracker信息
    IOTracker* tracker_;

    // closure的request信息
    RequestContext* reqCtx_;

    // metric信息
    FileMetric* metric_;

    // 起始时间
    uint64_t starttime_;

    // 重试次数
    uint64_t retryTimes_;

    // 当前closure归属于哪个iomanager
    IOManagerID managerID_;

    // 当前closure属于的iomanager
    IOManager* ioManager_;

    // 下一次rpc超时时间
    uint64_t nextTimeoutMS_;
};
}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_REQUEST_CLOSURE_H_

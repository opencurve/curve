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

#include "include/curve_compiler_specific.h"
#include "src/client/client_common.h"
#include "src/client/client_metric.h"
#include "src/client/inflight_controller.h"
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace client {

class IOTracker;
class IOManager;
class RequestScheduler;
struct FileMetric;
struct RequestContext;

class CURVE_CACHELINE_ALIGNMENT RequestClosure
    : public ::google::protobuf::Closure {
 public:
    explicit RequestClosure(RequestContext* reqctx) : reqCtx_(reqctx) {}
    virtual ~RequestClosure() = default;

    void Run() override;

    /**
     * @brief Get the inflight token before sending rpc
     */
    void GetInflightRPCToken();

    /**
     * @brief Release the inflight token when rpc returned
     */
    void ReleaseInflightRPCToken();

    /**
     * @brief Get error code
     */
    virtual int GetErrorCode() {
        return errcode_;
    }

    /**
     * @brief Set error code, 0 means success
     */
    virtual void SetFailed(int errorCode) {
        errcode_ = errorCode;
    }

    /**
     * @brief 获取当前closure属于哪个request
     */
    virtual RequestContext* GetReqCtx() {
        return reqCtx_;
    }

    /**
     * @brief 获取当前request属于哪个iotracker
     */
    virtual IOTracker* GetIOTracker() {
        return tracker_;
    }

    /**
     * @brief 设置当前属于哪一个iotracker
     */
    void SetIOTracker(IOTracker* ioTracker) {
        tracker_ = ioTracker;
    }

    /**
     * @brief 设置所属的iomanager
     */
    void SetIOManager(IOManager* ioManager) {
        ioManager_ = ioManager;
    }

    /**
     * @brief 设置当前closure重试次数
     */
    void IncremRetriedTimes() {
        retryTimes_++;
    }

    uint64_t GetRetriedTimes() const {
        return retryTimes_;
    }

    /**
     * 设置metric
     */
    void SetFileMetric(FileMetric* fm) {
        metric_ = fm;
    }

    /**
     * 获取metric指针
     */
    FileMetric* GetMetric() const {
        return metric_;
    }

    /**
     * 获取下一次rpc超时时间, rpc超时时间实现了指数退避的策略
     */
    uint64_t GetNextTimeoutMS() const {
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

    bool IsSuspendRPC() const {
        return suspendRPC_;
    }

 private:
    // suspend io标志
    bool suspendRPC_ = false;

    // whether own inflight count
    bool ownInflight_ = false;

    // 当前request的错误码
    int errcode_ = -1;

    // 当前request的tracker信息
    IOTracker* tracker_ = nullptr;

    // closure的request信息
    RequestContext* reqCtx_ = nullptr;

    // metric信息
    FileMetric* metric_ = nullptr;

    // 重试次数
    uint64_t retryTimes_ = 0;

    // 当前closure属于的iomanager
    IOManager* ioManager_ = nullptr;

    // 下一次rpc超时时间
    uint64_t nextTimeoutMS_ = 0;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_REQUEST_CLOSURE_H_

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

/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 */

#include "nebd/src/part1/async_request_closure.h"

#include <glog/logging.h>
#include <bthread/bthread.h>

#include <algorithm>
#include <memory>

namespace nebd {
namespace client {

void AsyncRequestClosure::Run() {
    auto span = tracer_->StartSpan("AsyncRequestClosure::Run");
    std::unique_ptr<AsyncRequestClosure> selfGuard(this);

    bool isCntlFailed = cntl.Failed();
    if (isCntlFailed) {
        ++aioCtx->retryCount;
        int64_t sleepUs = GetRpcRetryIntervalUs(aioCtx->retryCount);
        LOG_EVERY_SECOND(WARNING)
            << OpTypeToString(aioCtx->op) << " rpc failed"
            << ", error = " << cntl.ErrorText()
            << ", fd = " << fd
            << ", log id = " << cntl.log_id()
            << ", retryCount = " << aioCtx->retryCount
            << ", sleep " << (sleepUs / 1000) << " ms";
        bthread_usleep(sleepUs);
        Retry();
        span->End();
    } else {
        auto retCode = GetResponseRetCode();
        if (nebd::client::RetCode::kOK == retCode) {
            DVLOG(6) << OpTypeToString(aioCtx->op) << " success, fd = " << fd;

            // 读请求复制数据
            if (aioCtx->op == LIBAIO_OP::LIBAIO_OP_READ) {
                cntl.response_attachment().copy_to(
                    aioCtx->buf, cntl.response_attachment().size());
            }

            aioCtx->ret = 0;
            aioCtx->cb(aioCtx);
            span->SetStatus(trace::StatusCode::kOk);
            span->End();
        } else {
            LOG(ERROR) << OpTypeToString(aioCtx->op) << " failed, fd = " << fd
                       << ", offset = " << aioCtx->offset
                       << ", length = " << aioCtx->length
                       << ", retCode = " << GetResponseRetCode()
                       << ", log id = " << cntl.log_id();
            aioCtx->ret = -1;
            aioCtx->cb(aioCtx);
            span->SetStatus(trace::StatusCode::kError);
            span->End();
        }
    }
}

int64_t AsyncRequestClosure::GetRpcRetryIntervalUs(int64_t retryCount) const {
    // EHOSTDOWN: 找不到可用的server。
    // server可能停止服务了，也可能正在退出中(返回了ELOGOFF)
    if (cntl.ErrorCode() == EHOSTDOWN) {
        return requestOption_.rpcHostDownRetryIntervalUs;
    }

    if (retryCount <= 1) {
        return requestOption_.rpcRetryIntervalUs;
    }

    return std::max(
        requestOption_.rpcRetryIntervalUs,
        std::min(requestOption_.rpcRetryIntervalUs * retryCount,
                 requestOption_.rpcRetryMaxIntervalUs));
}

void AsyncRequestClosure::Retry() const {
    switch (aioCtx->op) {
        case LIBAIO_OP::LIBAIO_OP_WRITE:
            nebdClient.AioWrite(fd, aioCtx);
            break;
        case LIBAIO_OP::LIBAIO_OP_READ:
            nebdClient.AioRead(fd, aioCtx);
            break;
        case LIBAIO_OP::LIBAIO_OP_FLUSH:
            nebdClient.Flush(fd, aioCtx);
            break;
        case LIBAIO_OP::LIBAIO_OP_DISCARD:
            nebdClient.Discard(fd, aioCtx);
            break;
        default:
            LOG(ERROR) << "Aio Operation Type error, op = " << aioCtx->op
                       << ", fd = " << fd;
            aioCtx->ret = -1;
            aioCtx->cb(aioCtx);
    }
}

}  // namespace client
}  // namespace nebd

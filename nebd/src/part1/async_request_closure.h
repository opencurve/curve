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

#ifndef NEBD_SRC_PART1_ASYNC_REQUEST_CLOSURE_H_
#define NEBD_SRC_PART1_ASYNC_REQUEST_CLOSURE_H_

#include <brpc/controller.h>

#include "nebd/src/part1/nebd_client.h"
#include "nebd/src/part1/nebd_common.h"
#include "src/common/telemetry/telemetry.h"

namespace nebd {
namespace client {

struct AsyncRequestClosure : public google::protobuf::Closure {
    AsyncRequestClosure(int fd,
                        NebdClientAioContext* ctx,
                        const RequestOption& option)
      : fd(fd),
        aioCtx(ctx),
        requestOption_(option) {}
    AsyncRequestClosure(int fd, NebdClientAioContext *ctx,
                        const RequestOption &option,
                        nostd::shared_ptr<trace::Tracer> tracer)
        : fd(fd), aioCtx(ctx), requestOption_(option), tracer_(tracer) {}

    void Run() override;

    virtual RetCode GetResponseRetCode() const = 0;

    int64_t GetRpcRetryIntervalUs(int64_t retryCount) const;

    void Retry() const;

    // 请求fd
    int fd;

    // 请求上下文信息
    NebdClientAioContext* aioCtx;

    // brpc请求的controller
    brpc::Controller cntl;

    RequestOption requestOption_;

    nostd::shared_ptr<trace::Tracer> tracer_{curve::telemetry::GetNoopTracer()};
};

struct AioWriteClosure : public AsyncRequestClosure {
    AioWriteClosure(int fd,
                    NebdClientAioContext* ctx,
                    const RequestOption& option)
      : AsyncRequestClosure(
          fd,
          ctx,
          option) {}

    WriteResponse response;

    RetCode GetResponseRetCode() const override {
        return response.retcode();
    }
};

struct AioReadClosure : public AsyncRequestClosure {
    AioReadClosure(int fd,
                   NebdClientAioContext* ctx,
                   const RequestOption& option)
      : AsyncRequestClosure(
          fd,
          ctx,
          option) {}

    AioReadClosure(int fd, NebdClientAioContext *ctx,
                   const RequestOption &option,
                   nostd::shared_ptr<trace::Tracer> tracer
                   )
        : AsyncRequestClosure(fd, ctx, option, tracer) {}

    ReadResponse response;

    RetCode GetResponseRetCode() const override {
        return response.retcode();
    }
};

struct AioDiscardClosure : public AsyncRequestClosure {
    AioDiscardClosure(int fd,
                      NebdClientAioContext* ctx,
                      const RequestOption& option)
      : AsyncRequestClosure(
          fd,
          ctx,
          option) {}

    DiscardResponse response;

    RetCode GetResponseRetCode() const override {
        return response.retcode();
    }
};

struct AioFlushClosure : public AsyncRequestClosure {
    AioFlushClosure(int fd,
                    NebdClientAioContext* ctx,
                    const RequestOption& option)
      : AsyncRequestClosure(
          fd,
          ctx,
          option) {}

    FlushResponse response;

    RetCode GetResponseRetCode() const override {
        return response.retcode();
    }
};

inline const char* OpTypeToString(LIBAIO_OP opType) {
    switch (opType) {
    case LIBAIO_OP::LIBAIO_OP_READ:
        return "Read";
    case LIBAIO_OP::LIBAIO_OP_WRITE:
        return "Write";
    case LIBAIO_OP::LIBAIO_OP_DISCARD:
        return "Discard";
    case LIBAIO_OP::LIBAIO_OP_FLUSH:
        return "Flush";
    default:
        return "Unknown";
    }
}

}  // namespace client
}  // namespace nebd

#endif  // NEBD_SRC_PART1_ASYNC_REQUEST_CLOSURE_H_

/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART1_ASYNC_REQUEST_CLOSURE_H_
#define SRC_PART1_ASYNC_REQUEST_CLOSURE_H_

#include <brpc/controller.h>

#include "src/part1/nebd_client.h"
#include "src/part1/nebd_common.h"

namespace nebd {
namespace client {

struct AsyncRequestClosure : public google::protobuf::Closure {
    AsyncRequestClosure(int fd,
                        NebdClientAioContext* ctx,
                        const RequestOption& option)
      : fd(fd),
        aioCtx(ctx),
        requestOption_(option) {}

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

#endif  // SRC_PART1_ASYNC_REQUEST_CLOSURE_H_

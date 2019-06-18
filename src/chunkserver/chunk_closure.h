/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CHUNKSERVER_CHUNK_CLOSURE_H_
#define SRC_CHUNKSERVER_CHUNK_CLOSURE_H_

#include <brpc/closure_guard.h>
#include <memory>

#include "src/chunkserver/op_request.h"

namespace curve {
namespace chunkserver {

/**
 * 携带op request的所有上下文的closure，通过braft::Task传递给raft处理，
 * 调用会有两个地方：
 * 1.op request正常的被raft处理，最后on apply的时候会调用返回
 * 2.op request被打包给raft处理之后，但是还没有来得及处理就出错了，例如leader
 *   step down变为了非leader，那么会明确的提前向client返回错误
 */
class ChunkClosure : public braft::Closure {
 public:
    explicit ChunkClosure(std::shared_ptr<ChunkOpRequest> request)
        : request_(request) {}

    ~ChunkClosure() = default;

    void Run() override;

 public:
    // 包含了op request 的上下文信息
    std::shared_ptr<ChunkOpRequest> request_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNK_CLOSURE_H_

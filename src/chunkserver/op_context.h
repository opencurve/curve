/*
 * Project: curve
 * Created Date: 18-12-11
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_OP_CONTEXT_H
#define CURVE_CHUNKSERVER_OP_CONTEXT_H

#include <google/protobuf/message.h>
#include <butil/iobuf.h>

#include <memory>

#include "proto/chunk.pb.h"
#include "src/chunkserver/copyset_node.h"

namespace curve {
namespace chunkserver {

enum class RequestType {
    CHUNK_OP = 0,            // chunk file
    CHUNK_SNAPSHOT_OP = 1,   // chunk snapshot
    UNKNOWN_OP = 2
};

class ChunkOpContext {
 public:
    ChunkOpContext(RpcController *cntl,
                   const ChunkRequest *request,
                   ChunkResponse *response,
                   Closure *done) :
        cntl_(cntl),
        request_(request),
        response_(response),
        done_(done) {}

    /* 它的成员的生命周期都归rpc框架管，所以自己不会主动去析构 */
    virtual ~ChunkOpContext() {}

    ::google::protobuf::Closure *GetClosure() { return done_; }
    ChunkResponse *GetResponse() { return response_; }

    // Op 序列化
    // |                    data                   |
    // | op meta                      |   op data  |
    // | op type | op request length  | op request |
    // | 8 bit   |     32 bit         |  ....      |
    int Encode(butil::IOBuf *data);
    /* 节点正常，是从内存 request 上下文获取 op 信息进行 apply */
    int OnApply(std::shared_ptr<CopysetNode> copysetNode);
    /* 节点重启，是从 op log 反序列化获取 Op 信息进行 apply */
    static void OnApply(std::shared_ptr<CopysetNode> copysetNode,
                        butil::IOBuf *log);

 private:
    RpcController       *cntl_;
    const ChunkRequest  *request_;
    ChunkResponse       *response_;
    Closure             *done_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_OP_CONTEXT_H

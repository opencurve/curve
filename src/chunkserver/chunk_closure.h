/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_CHUNK_CLOSURE_H
#define CURVE_CHUNKSERVER_CHUNK_CLOSURE_H

#include <brpc/closure_guard.h>

#include "src/chunkserver/copyset_node.h"
#include "src/chunkserver/op_context.h"

namespace curve {
namespace chunkserver {

/* Implements Closure which encloses RPC stuff */
class ChunkClosure : public braft::Closure {
 public:
    ChunkClosure(CopysetNode *node, ChunkOpContext *opCtx)
        : copysetNode_(node), opCtx_(opCtx) {}

    ~ChunkClosure() {
        delete opCtx_;
        opCtx_ = nullptr;
    }
    ChunkOpContext *GetOpContext() {
        return opCtx_;
    }
    void Run();

 private:
    CopysetNode     *copysetNode_;
    ChunkOpContext  *opCtx_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_CHUNK_CLOSURE_H

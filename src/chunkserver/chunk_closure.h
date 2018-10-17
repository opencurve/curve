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
#include "src/chunkserver/op_request.h"

namespace curve {
namespace chunkserver {

/* Implements Closure which encloses RPC stuff */
class ChunkClosure : public braft::Closure {
 public:
    ChunkClosure(CopysetNode *node, ChunkOpRequest *request)
        : copysetNode_(node), request_(request) {}

    ~ChunkClosure() {
        delete request_;
        request_ = nullptr;
    }
    ChunkOpRequest *GetOpRequest() {
        return request_;
    }
    void Run();

 private:
    CopysetNode *copysetNode_;
    /* request 的生命周期归属 ChunkOpRequest 管 */
    ChunkOpRequest *request_;
};

class ChunkSnapshotClosure : public braft::Closure {
 public:
    ChunkSnapshotClosure(CopysetNode *node, ChunkSnapshotOpRequest *request)
        : copysetNode_(node), request_(request) {}

    ~ChunkSnapshotClosure() {
        delete request_;
        request_ = nullptr;
    }
    ChunkSnapshotOpRequest *GetOpRequest() {
        return request_;
    }
    void Run();

 private:
    CopysetNode *copysetNode_;
    // request 的生命周期归属 ChunkSnapshotClosure 管
    ChunkSnapshotOpRequest *request_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_CHUNK_CLOSURE_H

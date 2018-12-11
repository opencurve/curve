/*
 * Project: curve
 * Created Date: 18-10-11
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/chunkserver/chunk_closure.h"

namespace curve {
namespace chunkserver {

void ChunkClosure::Run() {
    /* Auto delete this after Run() */
    std::unique_ptr<ChunkClosure> selfGuard(this);
    /* Repsond this RPC. */
    brpc::ClosureGuard doneGuard(opCtx_->GetClosure());
    if (status().ok()) {
        return;
    }
    /* leader step down */
    if (EPERM == status().error_code()) {
        copysetNode_->RedirectChunkRequest(opCtx_->GetResponse());
    }
}

}  // namespace chunkserver
}  // namespace curve

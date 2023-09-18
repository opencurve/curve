/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * File Created: 2022-06-30
 * Author: xuchaojie
 */

#include "src/client/chunkserver_client.h"

#include <brpc/channel.h>
#include <bthread/bthread.h>
#include <algorithm>
#include <memory>

using curve::chunkserver::ChunkService_Stub;
using curve::chunkserver::CHUNK_OP_STATUS;

namespace curve {
namespace client {

int ChunkServerClient::UpdateFileEpoch(
    const CopysetPeerInfo &cs, uint64_t fileId, uint64_t epoch,
    ChunkServerClientClosure *done) {
    brpc::ClosureGuard doneGuard(done);
    std::unique_ptr<UpdateEpochContext> ctx(new UpdateEpochContext);

    int ret = 0;
    if (!cs.externalAddr.IsEmpty()) {
        ret = ctx->channel.Init(cs.externalAddr.addr_, NULL);
        if (ret != 0) {
            LOG(ERROR) << "failed to init channel to chunkserver, "
                       << "id: " << cs.chunkserverID
                       << ", "<< cs.externalAddr.addr_.ip
                       << ":" << cs.externalAddr.addr_.port;
            return -LIBCURVE_ERROR::INTERNAL_ERROR;
        }
    } else {
        ret = ctx->channel.Init(cs.internalAddr.addr_, NULL);
        if (ret != 0) {
            LOG(ERROR) << "failed to init channel to chunkserver, "
                       << "id: " << cs.chunkserverID
                       << ", "<< cs.internalAddr.addr_.ip
                       << ":" << cs.internalAddr.addr_.port;
            return -LIBCURVE_ERROR::INTERNAL_ERROR;
        }
    }

    ctx->cntl.set_timeout_ms(retryOps_.rpcTimeoutMs);

    ctx->request.set_fileid(fileId);
    ctx->request.set_epoch(epoch);

    ctx->done = done;
    ctx->curTry = 1;  // 1 for current try
    ctx->retryOps_ = retryOps_;

    google::protobuf::Closure* rpcDone = brpc::NewCallback(
                OnUpdateFileEpochReturned, ctx.get());

    ChunkService_Stub stub(&ctx->channel);
    stub.UpdateEpoch(&ctx->cntl, &ctx->request,
                     &ctx->response, rpcDone);
    doneGuard.release();
    ctx.release();
    return LIBCURVE_ERROR::OK;
}

void ChunkServerClient::OnUpdateFileEpochReturned(
    UpdateEpochContext *ctx) {
    std::unique_ptr<UpdateEpochContext> ctx_guard(ctx);
    brpc::ClosureGuard doneGuard(ctx->done);
    if (ctx->cntl.Failed()) {
        if (ctx->cntl.ErrorCode() == brpc::ELOGOFF ||
            ctx->cntl.ErrorCode() == EHOSTDOWN ||
            ctx->cntl.ErrorCode() == ECONNRESET ||
            ctx->cntl.ErrorCode() == ECONNREFUSED) {
            LOG(INFO) << "UpdateEpoch unable to contact chunkserver.";
            ctx->done->SetErrCode(LIBCURVE_ERROR::OK);
        } else if (ctx->curTry < ctx->retryOps_.rpcMaxTry) {
            bthread_usleep(ctx->retryOps_.rpcIntervalUs);
            ctx->cntl.Reset();
            uint32_t nextTimeOutMs =
                std::min(ctx->retryOps_.rpcTimeoutMs << ctx->curTry,
                         ctx->retryOps_.rpcMaxTimeoutMs);
            ctx->cntl.set_timeout_ms(nextTimeOutMs);
            ctx->curTry++;
            ctx->response.Clear();
            google::protobuf::Closure* rpcDone = brpc::NewCallback(
                        OnUpdateFileEpochReturned, ctx_guard.get());

            ChunkService_Stub stub(&ctx->channel);
            stub.UpdateEpoch(&ctx->cntl, &ctx->request,
                             &ctx->response, rpcDone);
            doneGuard.release();
            ctx_guard.release();
        } else {
            LOG(WARNING) << "UpdateEpoch failed, cntl.errotText: "
                         << ctx->cntl.ErrorText();
            ctx->done->SetErrCode(-LIBCURVE_ERROR::FAILED);
        }
    } else {
        switch (ctx->response.status()) {
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS:
                LOG(INFO) << "Received UpdateEpoch Response [log_id="
                          << ctx->cntl.log_id()
                          << "] from " << ctx->cntl.remote_side()
                          << " to " << ctx->cntl.local_side()
                          << ", Message: "
                          << ctx->response.DebugString();
                ctx->done->SetErrCode(LIBCURVE_ERROR::OK);
                break;
            case CHUNK_OP_STATUS::CHUNK_OP_STATUS_EPOCH_TOO_OLD:
                LOG(WARNING) << "Received UpdateEpoch Response [log_id="
                             << ctx->cntl.log_id()
                             << "] from " << ctx->cntl.remote_side()
                             << " to " << ctx->cntl.local_side()
                             << ", Message: "
                             << ctx->response.DebugString();
                ctx->done->SetErrCode(-LIBCURVE_ERROR::EPOCH_TOO_OLD);
                break;
            default:
                LOG(ERROR) << "can't reach here!";
                ctx->done->SetErrCode(-LIBCURVE_ERROR::UNKNOWN);
                break;
        }
    }
}

}   // namespace client
}   // namespace curve

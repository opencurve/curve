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
 * Created Date: 18-10-11
 * Author: wudemiao
 */

#include "src/chunkserver/chunk_closure.h"

#include <memory>

namespace curve {
namespace chunkserver {

void ChunkClosure::Run() {
    /**
     * After the completion of Run, automatically destruct
     * itself to prevent any missed destructor calls.
     */
    std::unique_ptr<ChunkClosure> selfGuard(this);
    /**
     * Ensure that done can be called to ensure that rpc will definitely return
     */
    brpc::ClosureGuard doneGuard(request_->Closure());
    /**
     * Although the identity of the leader has been confirmed
     * before proposing the request to the copyset, during the
     * processing of the request by the copyset, the current
     * identity of the copyset may still change to a non-leader.
     * Therefore, it is necessary to check the status of the
     * request when ChunkClosure is invoked. If it is 'ok', it
     * indicates a normal apply processing; otherwise, the
     * request should be forwarded.
     */
    if (status().ok()) {
        return;
    }

    request_->RedirectChunkRequest();
}

void ScanChunkClosure::Run() {
    // after run destory
    std::unique_ptr<ScanChunkClosure> selfGuard(this);
    std::unique_ptr<ChunkRequest> requestGuard(request_);
    std::unique_ptr<ChunkResponse> responseGuard(response_);

    switch (response_->status()) {
        case CHUNK_OP_STATUS_CHUNK_NOTEXIST:
            LOG(WARNING) << "scan chunk failed, read chunk not exist. "
                         << request_->ShortDebugString();
            break;
        case CHUNK_OP_STATUS_FAILURE_UNKNOWN:
            LOG(ERROR) << "scan chunk failed, read chunk unknown failure. "
                       << request_->ShortDebugString();
            break;
        default:
            break;
    }
}

void SendScanMapClosure::Guard() {
    std::unique_ptr<SendScanMapClosure> selfGuard(this);
    std::unique_ptr<FollowScanMapRequest> requestGuard(request_);
    std::unique_ptr<FollowScanMapResponse> responseGuard(response_);
    std::unique_ptr<brpc::Controller> cntlGuard(cntl_);
    std::unique_ptr<brpc::Channel> channelGuard(channel_);
}

void SendScanMapClosure::Run() {
    if (cntl_->Failed()) {
        if (retry_ > 0) {
            LOG(WARNING) << "Send scanmap to leader rpc failed."
                         << " cntl errorCode: " << cntl_->ErrorCode()
                         << " cntl error: " << cntl_->ErrorText()
                         << ", then will retry " << retry_ << " times.";
            retry_--;
            bthread_usleep(retryIntervalUs_);
            cntl_->Reset();
            cntl_->set_timeout_ms(rpcTimeoutMs_);
            ScanService_Stub stub(channel_);
            stub.FollowScanMap(cntl_, request_, response_, this);
        } else {
            LOG(ERROR) << " Send scanmap to leader rpc failed after retry,"
                       << " cntl errorCode: " << cntl_->ErrorCode()
                       << " cntl error: " << cntl_->ErrorText();
            Guard();
        }
    } else {
        Guard();
    }
}

}  // namespace chunkserver
}  // namespace curve

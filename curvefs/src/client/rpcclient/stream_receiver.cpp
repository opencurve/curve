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
 * Project: Curve
 * Created Date: 2022-03-15
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/rpcclient/stream_receiver.h"

namespace curvefs {
namespace client {
namespace rpcclient {

StreamReceiver::StreamReceiver()
    : streamId_(-1),
      finished_(true),
      status_(StreamStatus::RECEIVE_EOF) {}

bool StreamReceiver::Open(brpc::Controller* cntl,
                          StreamOptions options,
                          ReceiveCallback callback) {
    brpc::StreamOptions streamOptions;
    streamOptions.handler = this;
    streamOptions.idle_timeout_ms = options.idleTimeoutMs;
    if (brpc::StreamCreate(&streamId_, *cntl, &streamOptions) != 0) {
        LOG(ERROR) << "Fail to create stream";
        return false;
    }

    finished_ = false;
    callback_ = callback;
    status_ = StreamStatus::RECEIVE_OK;
    return true;
}

bool StreamReceiver::Close() {
    WaitFinished();
    if (status_ != StreamStatus::RECEIVE_EOF) {
        LOG(ERROR) << "stream=" << streamId_ << " not all data received yet";
        return false;
    } else if (brpc::StreamClose(streamId_) != 0) {
        LOG(ERROR) << "Close stream=" << streamId_ << " failed";
        return false;
    }
    return true;
}

int StreamReceiver::on_received_messages(brpc::StreamId id,
                                         butil::IOBuf* const buffers[],
                                         size_t size) {
    for (size_t i = 0; i < size; i++) {
        status_ = callback_(buffers[i]);
        if (status_ == StreamStatus::RECEIVE_EOF ||
            status_ == StreamStatus::RECEIVE_ERROR) {
            break;
        }
        // RECEIVER_OK, continue
    }

    if (status_ == StreamStatus::RECEIVE_EOF ||
        status_ == StreamStatus::RECEIVE_ERROR) {
        Finished();
    }
    return 0;
}

void StreamReceiver::on_idle_timeout(brpc::StreamId id) {
    LOG(ERROR) << "Stream=" << id << " has no data transmission for a while";
    status_ = StreamStatus::RECEIVE_ERROR;
    Finished();
}

void StreamReceiver::on_closed(brpc::StreamId id) {
    LOG(ERROR) << "Stream=" << id << " is closed";
    status_ = StreamStatus::RECEIVE_ERROR;
    Finished();
}

inline void StreamReceiver::Finished() {
    std::unique_lock<std::mutex> ul(mtx_);
    finished_ = true;
    cond_.notify_one();
}

inline void StreamReceiver::WaitFinished() {
    std::unique_lock<std::mutex> ul(mtx_);
    cond_.wait(ul, [this]() { return finished_; });
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
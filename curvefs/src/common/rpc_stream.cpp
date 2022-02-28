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

#include "curvefs/src/common/rpc_stream.h"

#include <memory>

namespace curvefs {
namespace common {


StreamConnection::StreamConnection()
    : streamId_(-1),
      status_(StreamStatus::RECEIVE_EOF),
      finished_(true) {}

StreamConnection::StreamConnection(brpc::StreamId streamId,
                                   ReceiveCallback callback,
                                   StreamOptions options)
    : streamId_(streamId),
      callback_(callback),
      options_(options),
      status_(StreamStatus::RECEIVE_EOF),
      finished_(false) {}

brpc::StreamId StreamConnection::GetStreamId() {
    return streamId_;
}

bool StreamConnection::Write(const butil::IOBuf& buffer) {
    int rc = 0;
    bool tried = false;
    for ( ; ; ) {
        rc = brpc::StreamWrite(streamId_, buffer);
        if (rc == EAGAIN && !tried) {
            tried = true;
            auto nextTime = butil::milliseconds_from_now(
                options_.waitTimeoutMs);
            rc = brpc::StreamWait(streamId_, &nextTime);
            if (rc == 0) {  // wait success
                continue;
            }
        }
        break;
    }
    return rc == 0;
}

bool StreamConnection::WaitAllDataReceived() {
    WaitFinished();
    if (status_ != StreamStatus::RECEIVE_EOF) {
        LOG(ERROR) << "stream=" << streamId_ << " not all data received yet";
        return false;
    }
    return true;
}

int StreamConnection::on_received_messages(butil::IOBuf* const buffers[],
                                           size_t size) {
    if (nullptr == callback_) {
        return 0;
    }

    for (size_t i = 0; i < size; i++) {
        status_ = callback_(buffers[i]);
        if (status_ == StreamStatus::RECEIVE_EOF ||
            status_ == StreamStatus::RECEIVE_ERROR) {
            Finished();
            break;
        }
        // RECEIVER_OK, continue
    }
    return 0;
}

void StreamConnection::on_idle_timeout() {
    LOG(ERROR) << "Stream=" << streamId_
               << " has no data transmission for a while";
    status_ = StreamStatus::RECEIVE_ERROR;
    Finished();
}

void StreamConnection::on_closed() {
    LOG(INFO) << "Stream=" << streamId_ << " is closed";
}

inline void StreamConnection::Finished() {
    std::unique_lock<std::mutex> ul(mtx_);
    finished_ = true;
    cond_.notify_one();
}

inline void StreamConnection::WaitFinished() {
    std::unique_lock<std::mutex> ul(mtx_);
    cond_.wait(ul, [this]() { return finished_; });
}

void StreamHandler::Close(std::shared_ptr<StreamConnection> connection) {
    if (connection != nullptr) {
        connections_.erase(connection->GetStreamId());
    }
}

std::shared_ptr<StreamConnection> StreamHandler::GetConnection(
    brpc::StreamId id) {
    auto iter = connections_.find(id);
    if (iter == connections_.end()) {
        return nullptr;
    }
    return iter->second;
}

int StreamHandler::on_received_messages(brpc::StreamId id,
                                         butil::IOBuf* const buffers[],
                                         size_t size) {
    auto connection = GetConnection(id);
    if (connection != nullptr) {
        return connection->on_received_messages(buffers, size);
    }
    return 0;
}

void StreamHandler::on_idle_timeout(brpc::StreamId id) {
    auto connection = GetConnection(id);
    if (connection != nullptr) {
        connection->on_idle_timeout();
    }
}

void StreamHandler::on_closed(brpc::StreamId id) {
    auto connection = GetConnection(id);
    if (connection != nullptr) {
        connection->on_closed();
    }
}

std::shared_ptr<StreamConnection> StreamReceiver::Open(brpc::Controller* cntl,
                                                       ReceiveCallback callback,
                                                       StreamOptions options) {
    brpc::StreamId streamId;
    brpc::StreamOptions streamOptions;
    streamOptions.handler = this;
    streamOptions.idle_timeout_ms = options.idleTimeoutMs;
    if (brpc::StreamCreate(&streamId, *cntl, &streamOptions) != 0) {
        LOG(ERROR) << "Fail to create stream";
        return nullptr;
    }

    auto connection = std::make_shared<StreamConnection>(
        streamId, callback, options);
    connections_[streamId] = connection;
    return connection;
}

std::shared_ptr<StreamConnection> StreamSender::Open(brpc::Controller* cntl,
                                                     ReceiveCallback callback,
                                                     StreamOptions options) {
    brpc::StreamId streamId;
    if (brpc::StreamAccept(&streamId, *cntl, nullptr) != 0) {
        LOG(ERROR) << "Fail to accept stream";
        return nullptr;
    }

    auto connection = std::make_shared<StreamConnection>(
        streamId, callback, options);
    connections_[streamId] = connection;
    return connection;
}

}  // namespace common
}  // namespace curvefs

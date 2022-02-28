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

std::ostream& operator<<(std::ostream& os, const StreamStatus status) {
    switch (status) {
        case StreamStatus::STREAM_OK:
            os << "STREAM_OK";
            break;
        case StreamStatus::STREAM_ERROR:
            os << "STREAM_ERROR";
            break;
        case StreamStatus::STREAM_EOF:
            os << "STREAM_EOF";
            break;
        case StreamStatus::STREAM_TIMEOUT:
            os << "STREAM_TIMEOUT";
            break;
        case StreamStatus::STREAM_CLOSE:
            os << "STREAM_CLOSE";
            break;
        default:
            os << "STREAM_UNKNOWN";
            break;
    }
    return os;
}

const std::string StreamConnection::kEOFMessage_ = "__EOF__";  // NOLINT

StreamConnection::StreamConnection()
    : streamId_(-1),
      status_(StreamStatus::STREAM_OK) {}

StreamConnection::StreamConnection(brpc::StreamId streamId,
                                   const ReceiveCallback& callback)
    : streamId_(streamId),
      callback_(callback),
      status_(StreamStatus::STREAM_OK) {}

bool StreamConnection::Write(const butil::IOBuf& buffer) {
    return brpc::StreamWrite(streamId_, buffer) == 0;
}

bool StreamConnection::WriteDone() {
    butil::IOBuf buffer;
    buffer.append(GetEOFMessage());
    return brpc::StreamWrite(streamId_, buffer) == 0;
}

StreamStatus StreamConnection::WaitAllDataReceived() {
    Wait();
    if (status_ == StreamStatus::STREAM_EOF) {
        return StreamStatus::STREAM_OK;
    }
    return status_;
}

brpc::StreamId StreamConnection::GetStreamId() {
    return streamId_;
}

std::string StreamConnection::GetEOFMessage() {
    return kEOFMessage_;
}

void StreamConnection::SetStatus(StreamStatus status) {
    status_ = status;
}

bool StreamConnection::InvokeReceiveCallback(butil::IOBuf* buffer) {
    if (nullptr == callback_) {
        return false;
    }
    return callback_(buffer);
}

void StreamConnection::Wait() {
    std::unique_lock<std::mutex> ul(mtx_);
    cond_.wait(ul, [this]() {
        return status_ == StreamStatus::STREAM_EOF ||
            status_ == StreamStatus::STREAM_TIMEOUT ||
            status_ == StreamStatus::STREAM_CLOSE ||
            status_ == StreamStatus::STREAM_ERROR;
    });
}

void StreamConnection::Notify() {
    std::unique_lock<std::mutex> ul(mtx_);
    cond_.notify_one();
}

StreamClient::~StreamClient() {
    for (const auto& item : connections_) {
        auto connection = item.second;
        if (connection != nullptr) {
            brpc::StreamClose(connection->GetStreamId());
        }
    }
}

std::shared_ptr<StreamConnection> StreamClient::Connect(
    brpc::Controller* cntl,
    const ReceiveCallback& callback,
    StreamOptions options) {
    brpc::StreamId streamId;
    brpc::StreamOptions streamOptions;
    streamOptions.handler = this;
    streamOptions.idle_timeout_ms = options.idleTimeoutMs;
    if (brpc::StreamCreate(&streamId, *cntl, &streamOptions) != 0) {
        LOG(ERROR) << "Failed to create stream in client-side";
        return nullptr;
    }

    auto connection = std::make_shared<StreamConnection>(streamId, callback);
    {
        std::lock_guard<std::mutex> lg(mtx_);
        connections_[streamId] = connection;
    }
    return connection;
}

void StreamClient::Close(std::shared_ptr<StreamConnection> connection) {
    if (nullptr == connection) {
        LOG(ERROR) << "connection is null";
        return;
    }

    brpc::StreamId streamId = connection->GetStreamId();
    if (brpc::StreamClose(streamId) != 0) {
        LOG(ERROR) << "stream (streamId=" << streamId
                   << ") close failed in client-side";
    }

    std::lock_guard<std::mutex> lg(mtx_);
    auto iter = connections_.find(streamId);
    if (iter == connections_.end()) {
        LOG(ERROR) << "connection (streamId=" << streamId << ") not found";
    } else {
        connections_.erase(iter);
    }
}

std::shared_ptr<StreamConnection> StreamClient::GetConnection(
    brpc::StreamId id) {
    {
        std::lock_guard<std::mutex> lg(mtx_);
        auto iter = connections_.find(id);
        if (iter != connections_.end()) {
            return iter->second;
        }
    }

    // connection not found
    if (brpc::StreamClose(id) != 0) {
        LOG(ERROR) << "stream (streamId=" << id
                   << ") close failed in client-side";
    }
    return nullptr;
}

int StreamClient::on_received_messages(brpc::StreamId id,
                                       butil::IOBuf* const buffers[],
                                       size_t size) {
    auto connection = GetConnection(id);
    if (nullptr == connection) {
        LOG(ERROR) << "on_received_messages: connection not found";
        return 0;
    }

    StreamStatus status = StreamStatus::STREAM_OK;
    std::string eofMessage = connection->GetEOFMessage();
    for (size_t i = 0; i < size; i++) {
        butil::IOBuf* buffer = buffers[i];
        if (buffer->size() == eofMessage.size() &&
            buffer->to_string() == eofMessage) {
            status = StreamStatus::STREAM_EOF;
            break;
        } else if (!connection->InvokeReceiveCallback(buffer)) {
            status = StreamStatus::STREAM_ERROR;
            break;
        }
    }

    if (status != StreamStatus::STREAM_OK) {
        connection->SetStatus(status);
        connection->Notify();
    }
    return 0;
}

void StreamClient::on_idle_timeout(brpc::StreamId id) {
    auto connection = GetConnection(id);
    if (connection != nullptr) {
        connection->SetStatus(StreamStatus::STREAM_TIMEOUT);
        connection->Notify();
    }
}

void StreamClient::on_closed(brpc::StreamId id) {
    auto connection = GetConnection(id);
    if (connection != nullptr) {
        connection->SetStatus(StreamStatus::STREAM_CLOSE);
        connection->Notify();
    }
}

StreamServer::~StreamServer() {
    for (const auto& item : streamIds_) {
        brpc::StreamId streamId = item.first;
        brpc::StreamClose(streamId);
    }
}

std::shared_ptr<StreamConnection> StreamServer::Accept(brpc::Controller* cntl) {
    brpc::StreamId streamId;
    brpc::StreamOptions streamOptions;
    streamOptions.handler = this;
    streamOptions.max_buf_size = 0;  // unlimited buffer size
    if (brpc::StreamAccept(&streamId, *cntl, &streamOptions) != 0) {
        LOG(ERROR) << "Failed to accept stream in server-side";
        return nullptr;
    }

    {
        std::lock_guard<std::mutex> lg(mtx_);
        streamIds_[streamId] = true;
    }
    return std::make_shared<StreamConnection>(streamId, nullptr);
}

int StreamServer::on_received_messages(brpc::StreamId id,
                                       butil::IOBuf* const buffers[],
                                       size_t size) {
    LOG(ERROR) << "on_received_messages: stream (streamId=" << id
               << ") in server-side should not reveice any message"
               << ", but now we received";
    return 0;
}

void StreamServer::on_idle_timeout(brpc::StreamId id) {
    LOG(ERROR) << "on_idle_timeout: stream (streamId=" << id
               << ") in server-side should not wait for data receive";
}

void StreamServer::on_closed(brpc::StreamId id) {
    if (brpc::StreamClose(id) != 0) {
        LOG(ERROR) << "on_closed: stream (streamId=" << id
                   << ") close failed in server-side";
    } else {
        {
            std::lock_guard<std::mutex> lg(mtx_);
            streamIds_.erase(id);
        }
        VLOG(3) << "on_closed: stream (streamId=" << id
                << ") close success in server-side";
    }
}

}  // namespace common
}  // namespace curvefs

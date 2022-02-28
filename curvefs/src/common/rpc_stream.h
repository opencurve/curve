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

#include <glog/logging.h>
#include <brpc/channel.h>
#include <brpc/stream.h>

#include <mutex>
#include <string>
#include <memory>
#include <functional>
#include <unordered_map>
#include <condition_variable>

#ifndef CURVEFS_SRC_COMMON_RPC_STREAM_H_
#define CURVEFS_SRC_COMMON_RPC_STREAM_H_

namespace curvefs {
namespace common {

struct StreamOptions {
    StreamOptions()
        : idleTimeoutMs(500) {}

    explicit StreamOptions(uint64_t idleTimeoutMs)
        : idleTimeoutMs(idleTimeoutMs) {}

    uint64_t idleTimeoutMs;
};

enum class StreamStatus {
    STREAM_OK,
    STREAM_ERROR,
    STREAM_EOF,
    STREAM_TIMEOUT,
    STREAM_CLOSE,
    STREAM_UNKNOWN,
};

std::ostream& operator<<(std::ostream& os, const StreamStatus status);

using ReceiveCallback = std::function<bool(butil::IOBuf*)>;

class StreamConnection {
 public:
    friend class StreamClient;
    friend class StreamServer;

 public:
    StreamConnection();

    StreamConnection(brpc::StreamId streamId,
                     const ReceiveCallback& callback);

    bool Write(const butil::IOBuf& buffer);

    bool WriteDone();

    StreamStatus WaitAllDataReceived();

    brpc::StreamId GetStreamId();

 private:
    std::string GetEOFMessage();

    void SetStatus(StreamStatus status);

    bool InvokeReceiveCallback(butil::IOBuf*);

    void Wait();

    void Notify();

 private:
    static const std::string kEOFMessage_;

    brpc::StreamId streamId_;

    ReceiveCallback callback_;

    StreamStatus status_;

    std::mutex mtx_;

    std::condition_variable cond_;
};

class StreamClient : public brpc::StreamInputHandler {
 public:
    StreamClient() = default;

    ~StreamClient();

    std::shared_ptr<StreamConnection> Connect(brpc::Controller* cntl,
                                              const ReceiveCallback& callback,
                                              StreamOptions options);

    void Close(std::shared_ptr<StreamConnection> connection);

 private:
    std::shared_ptr<StreamConnection> GetConnection(brpc::StreamId id);

    int on_received_messages(brpc::StreamId id,
                             butil::IOBuf* const buffers[],
                             size_t size) override;

    void on_idle_timeout(brpc::StreamId id) override;

    void on_closed(brpc::StreamId id) override;

 private:
    std::mutex mtx_;
    std::unordered_map<brpc::StreamId,
                       std::shared_ptr<StreamConnection>> connections_;
};

class StreamServer : public brpc::StreamInputHandler {
 public:
    StreamServer() = default;

    ~StreamServer();

    std::shared_ptr<StreamConnection> Accept(brpc::Controller* cntl);

 private:
    int on_received_messages(brpc::StreamId id,
                             butil::IOBuf* const buffers[],
                             size_t size) override;

    void on_idle_timeout(brpc::StreamId id) override;

    void on_closed(brpc::StreamId id) override;

 private:
    std::mutex mtx_;
    std::unordered_map<brpc::StreamId, bool> streamIds_;
};

}  // namespace common
}  // namespace curvefs

#endif  // CURVEFS_SRC_COMMON_RPC_STREAM_H_

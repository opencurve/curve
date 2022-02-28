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


#include <map>
#include <mutex>
#include <string>
#include <memory>
#include <functional>
#include <condition_variable>

#ifndef CURVEFS_SRC_COMMON_RPC_STREAM_H_
#define CURVEFS_SRC_COMMON_RPC_STREAM_H_

namespace curvefs {
namespace common {

struct StreamOptions {
    StreamOptions()
        : max_buf_size(0),
          idleTimeoutMs(500),
          waitTimeoutMs(500) {}

    explicit StreamOptions(uint64_t idleTimeoutMs)
        : idleTimeoutMs(idleTimeoutMs) {}

    int max_buf_size;
    uint64_t idleTimeoutMs;
    uint64_t waitTimeoutMs;
};

enum class StreamStatus {
    RECEIVE_EOF,
    RECEIVE_OK,
    RECEIVE_ERROR,
};

using ReceiveCallback = std::function<StreamStatus(butil::IOBuf*)>;

class StreamConnection {
 public:
    StreamConnection();

    StreamConnection(brpc::StreamId streamId,
                     ReceiveCallback callback,
                     StreamOptions options);

    brpc::StreamId GetStreamId();

    bool Write(const butil::IOBuf& buffer);

    bool WaitAllDataReceived();

    int on_received_messages(butil::IOBuf* const buffers[],
                             size_t size);

    void on_idle_timeout();

    void on_closed();

 private:
    void Finished();

    void WaitFinished();

 private:
    brpc::StreamId streamId_;
    ReceiveCallback callback_;
    StreamOptions options_;
    StreamStatus status_;
    bool finished_;
    std::mutex mtx_;
    std::condition_variable cond_;
};

class StreamHandler : public brpc::StreamInputHandler {
 public:
    virtual std::shared_ptr<StreamConnection> Open(
        brpc::Controller* cntl,
        ReceiveCallback callback,
        StreamOptions options) = 0;

    void Close(std::shared_ptr<StreamConnection> connection);

 protected:
    std::shared_ptr<StreamConnection> GetConnection(brpc::StreamId id);

    int on_received_messages(brpc::StreamId id,
                             butil::IOBuf* const buffers[],
                             size_t size) override;

    void on_idle_timeout(brpc::StreamId id) override;

    void on_closed(brpc::StreamId id) override;

 protected:
    std::map<brpc::StreamId, std::shared_ptr<StreamConnection>> connections_;
};

class StreamReceiver : public StreamHandler {
 public:
    StreamReceiver() = default;

    std::shared_ptr<StreamConnection> Open(brpc::Controller* cntl,
                                           ReceiveCallback callback,
                                           StreamOptions options) override;
};

class StreamSender : public StreamHandler {
 public:
    StreamSender() = default;

    std::shared_ptr<StreamConnection> Open(brpc::Controller* cntl,
                                           ReceiveCallback callback,
                                           StreamOptions options) override;
};

inline static std::string GetStreamEOFString() {
    return "__EOF__";
}

}  // namespace common
}  // namespace curvefs

#endif  // CURVEFS_SRC_COMMON_RPC_STREAM_H_

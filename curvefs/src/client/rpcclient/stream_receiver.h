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
#include <functional>
#include <condition_variable>

#ifndef CURVEFS_SRC_CLIENT_RPCCLIENT_STREAM_RECEIVER_H_
#define CURVEFS_SRC_CLIENT_RPCCLIENT_STREAM_RECEIVER_H_

namespace curvefs {
namespace client {
namespace rpcclient {

enum class StreamStatus {
    RECEIVE_EOF,
    RECEIVE_OK,
    RECEIVE_ERROR,
};

struct StreamOptions {
    uint64_t idleTimeoutMs = 100;
};

using ReceiveCallback = function<StreamStatus(butil::IOBuf*)>;

class StreamReceiver : public brpc::StreamInputHandler {
 public:
    StreamReceiver();

    bool Open(brpc::Controller* cntl,
              StreamOptions options,
              ReceiveCallback callback);

    // close will wait all data received or encounter an error.
    bool Close();

 private:
    int on_received_messages(brpc::StreamId id,
                             butil::IOBuf* const buffers[],
                             size_t size) override;

    void on_idle_timeout(brpc::StreamId id) override;

    void on_closed(brpc::StreamId id) override;

    void Finished();

    void WaitFinished();

 private:
    ReceiveCallback callback_;
    brpc::StreamId streamId_;
    StreamStatus status_;
    bool finished_;
    std::mutex mtx_;
    std::condition_variable cond_;
};

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_STREAM_RECEIVER_H_

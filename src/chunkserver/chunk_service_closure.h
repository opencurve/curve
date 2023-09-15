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
 * Created Date: Thursday June 20th 2019
 * Author: yangyaokai
 */

#ifndef SRC_CHUNKSERVER_CHUNK_SERVICE_CLOSURE_H_
#define SRC_CHUNKSERVER_CHUNK_SERVICE_CLOSURE_H_

#include <brpc/closure_guard.h>
#include <memory>

#include "proto/chunk.pb.h"
#include "src/chunkserver/op_request.h"
#include "src/chunkserver/inflight_throttle.h"
#include "src/common/timeutility.h"

namespace curve {
namespace chunkserver {

// The closure of the chunk service layer encapsulates the closure of the rpc layer, which is used to count metric information when requesting returns
class ChunkServiceClosure : public braft::Closure {
 public:
    explicit ChunkServiceClosure(
            std::shared_ptr<InflightThrottle> inflightThrottle,
            const ChunkRequest *request,
            ChunkResponse *response,
            google::protobuf::Closure *done)
        : inflightThrottle_(inflightThrottle)
        , request_(request)
        , response_(response)
        , brpcDone_(done)
        , receivedTimeUs_(common::TimeUtility::GetTimeofDayUs()) {
            // What does the closure create add 1, and when the closure is called, subtract 1
            if (nullptr != inflightThrottle_) {
                inflightThrottle_->Increment();
            }
            // Count the number of requests
            OnRequest();
        }

    ~ChunkServiceClosure() = default;

    /**
     * This function will be called at the end of the guard lifecycle of the closure
     * Currently, this function mainly performs some metric statistics on the returned results of read and write requests
     * If there are similar scenarios in the future (doing some processing at the end of the service request), logic can be added internally
     */
    void Run() override;

 private:
    /**
     * Count the number and rate of requests
     */
    void OnRequest();
    /**
     * Record the results of request processing, such as whether the request was incorrect, the delay of the request, etc
     */
    void OnResonse();

 private:
    // inflight flow control
    std::shared_ptr<InflightThrottle> inflightThrottle_;
    // Request for rpc requests
    const ChunkRequest *request_;
    // Response to rpc requests
    ChunkResponse *response_;
    // Rpc request callback
    google::protobuf::Closure *brpcDone_;
    // Time of receiving the request
    uint64_t receivedTimeUs_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNK_SERVICE_CLOSURE_H_

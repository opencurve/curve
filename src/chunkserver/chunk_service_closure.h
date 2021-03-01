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

// chunk service layer closures, which encapsulates rpc closures,
// is used to count metric information on request return
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
            // Subtract 1 when closure is called, add 1 when closure is created
            if (nullptr != inflightThrottle_) {
                inflightThrottle_->Increment();
            }
            // Count requests
            OnRequest();
        }

    ~ChunkServiceClosure() = default;

    /**
     * This function is called at the end of the life of the closure's guard
     * This function currently focuses on some metric statistics for the
     * results returned from read and write requests
     * If there is a similar scenario (doing some processing at the end of a
     * service request) you can add logic to it internally
     */
    void Run() override;

 private:
    /**
     * count the number and rate of requests
     */
    void OnRequest();
    /**
     * Log the results of the request processing,
     * e.g. whether the request was in error, the delay of the request, etc.
     */
    void OnResonse();

 private:
    // inflight Throttle
    std::shared_ptr<InflightThrottle> inflightThrottle_;
    // Request of rpc request
    const ChunkRequest *request_;
    // Response of rpc request
    ChunkResponse *response_;
    // rpc request callback
    google::protobuf::Closure *brpcDone_;
    // Time of receiving the request
    uint64_t receivedTimeUs_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNK_SERVICE_CLOSURE_H_

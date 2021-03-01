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

#include "src/chunkserver/chunk_service_closure.h"
#include <memory>

#include "src/chunkserver/chunkserver_metrics.h"

namespace curve {
namespace chunkserver {

void ChunkServiceClosure::Run() {
    /**
     * After Run，deconstruct itself，to avoid the bugs of the deconstructor
     */
    std::unique_ptr<ChunkServiceClosure> selfGuard(this);

    {
        // All operations to be done before the brpcDone_ call are put
        // into this lifecycle
        brpc::ClosureGuard doneGuard(brpcDone_);
        // Log the results of the request processing and collect them
        // into the metric
        OnResonse();
    }

    // Subtract 1 when closure is called, add 1 when closure is created
    // This line must be placed after the brpcDone_ call. ut needs to
    // test the performance of inflightio when the limit is exceeded.
    // A sleep will be added to the incoming closure to control the
    // number of inflightio
    if (nullptr != inflightThrottle_) {
        inflightThrottle_->Decrement();
    }
}

void ChunkServiceClosure::OnRequest() {
    // If request or response is empty, metric is not counted
    if (request_ == nullptr || response_ == nullptr)
        return;

    // count the number of requests by request type
    ChunkServerMetric* metric = ChunkServerMetric::GetInstance();
    switch (request_->optype()) {
        case CHUNK_OP_TYPE::CHUNK_OP_READ: {
            metric->OnRequest(request_->logicpoolid(),
                              request_->copysetid(),
                              CSIOMetricType::READ_CHUNK);
            break;
        }
        case CHUNK_OP_TYPE::CHUNK_OP_WRITE: {
            metric->OnRequest(request_->logicpoolid(),
                              request_->copysetid(),
                              CSIOMetricType::WRITE_CHUNK);
            break;
        }
        case CHUNK_OP_TYPE::CHUNK_OP_RECOVER: {
            metric->OnRequest(request_->logicpoolid(),
                              request_->copysetid(),
                              CSIOMetricType::RECOVER_CHUNK);
            break;
        }
        case CHUNK_OP_TYPE::CHUNK_OP_PASTE: {
            metric->OnRequest(request_->logicpoolid(),
                              request_->copysetid(),
                              CSIOMetricType::PASTE_CHUNK);
            break;
        }
        default:
            break;
    }
}

void ChunkServiceClosure::OnResonse() {
    // If request or response is empty, metric is not counted
    if (request_ == nullptr || response_ == nullptr)
        return;

    // Count results of this request based on the return value in the response
    ChunkServerMetric* metric = ChunkServerMetric::GetInstance();
    bool hasError = false;
    uint64_t latencyUs =
        common::TimeUtility::GetTimeofDayUs() - receivedTimeUs_;
    switch (request_->optype()) {
        case CHUNK_OP_TYPE::CHUNK_OP_READ: {
            // If it is a read request, the return
            // CHUNK_OP_STATUS_CHUNK_NOTEXIST is also considered correct
            hasError = (response_->status()
                        != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS) &&
                        (response_->status()
                        != CHUNK_OP_STATUS::CHUNK_OP_STATUS_CHUNK_NOTEXIST);

            metric->OnResponse(request_->logicpoolid(),
                               request_->copysetid(),
                               CSIOMetricType::READ_CHUNK,
                               request_->size(),
                               latencyUs,
                               hasError);
            break;
        }
        case CHUNK_OP_TYPE::CHUNK_OP_WRITE: {
            hasError = response_->status()
                       != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS;
            metric->OnResponse(request_->logicpoolid(),
                               request_->copysetid(),
                               CSIOMetricType::WRITE_CHUNK,
                               request_->size(),
                               latencyUs,
                               hasError);
            break;
        }
        case CHUNK_OP_TYPE::CHUNK_OP_RECOVER: {
            hasError = response_->status()
                       != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS;
            metric->OnResponse(request_->logicpoolid(),
                               request_->copysetid(),
                               CSIOMetricType::RECOVER_CHUNK,
                               request_->size(),
                               latencyUs,
                               hasError);
            break;
        }
        case CHUNK_OP_TYPE::CHUNK_OP_PASTE: {
            hasError = response_->status()
                       != CHUNK_OP_STATUS::CHUNK_OP_STATUS_SUCCESS;
            metric->OnResponse(request_->logicpoolid(),
                               request_->copysetid(),
                               CSIOMetricType::PASTE_CHUNK,
                               request_->size(),
                               latencyUs,
                               hasError);
            break;
        }
        default:
            break;
    }
}

}  // namespace chunkserver
}  // namespace curve

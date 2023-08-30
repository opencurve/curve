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
     *Automatically deconstruct oneself after the end of Run, which can avoid
     *Destructor omission
     */
    std::unique_ptr<ChunkServiceClosure> selfGuard(this);

    {
        //All brpcDone_ All operations that need to be done before calling are placed within this lifecycle
        brpc::ClosureGuard doneGuard(brpcDone_);
        //Record the request processing results and collect them in metric
        OnResonse();
    }

    //When calling the closure, subtract 1, and add 1 to what the closure creates
    //This line must be placed in brpcDone_ After calling, UT needs to test the performance of inflightio when it exceeds the limit
    //Will add a sleep to the incoming closure to control the number of inflightio
    if (nullptr != inflightThrottle_) {
        inflightThrottle_->Decrement();
    }
}

void ChunkServiceClosure::OnRequest() {
    //If request or response is empty, metric will not be counted
    if (request_ == nullptr || response_ == nullptr)
        return;

    //Count the number of requests based on their type
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
    //If request or response is empty, metric will not be counted
    if (request_ == nullptr || response_ == nullptr)
        return;

    //The processing result of this request can be calculated based on the return value in the response
    ChunkServerMetric* metric = ChunkServerMetric::GetInstance();
    bool hasError = false;
    uint64_t latencyUs =
        common::TimeUtility::GetTimeofDayUs() - receivedTimeUs_;
    switch (request_->optype()) {
        case CHUNK_OP_TYPE::CHUNK_OP_READ: {
            //If it is a read request, return CHUNK_OP_STATUS_CHUNK_NOTEXIST also believes that it is correct
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

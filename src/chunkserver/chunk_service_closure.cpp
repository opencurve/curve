/*
 * Project: curve
 * Created Date: Thursday June 20th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#include "src/chunkserver/chunk_service_closure.h"
#include <memory>

#include "src/chunkserver/chunkserver_metrics.h"

namespace curve {
namespace chunkserver {

void ChunkServiceClosure::Run() {
    /**
     * 在Run结束之后，自动析构自己，这样可以避免
     * 析构函数漏调
     */
    std::unique_ptr<ChunkServiceClosure> selfGuard(this);

    {
        // 所有brpcDone_调用之前要做的操作都放到这个生命周期内
        brpc::ClosureGuard doneGuard(brpcDone_);
        // 记录请求处理结果，收集到metric中
        OnResonse();
    }

    // closure调用的时候减1，closure创建的什么加1
    // 这一行必须放在brpcDone_调用之后，ut里需要测试inflightio超过限制时的表现
    // 会在传进来的closure里面加一个sleep来控制inflightio个数
    if (nullptr != inflightThrottle_) {
        inflightThrottle_->Decrement();
    }
}

void ChunkServiceClosure::OnRequest() {
    // 如果request或者response为空就不统计metric
    if (request_ == nullptr || response_ == nullptr)
        return;

    // 根据request类型统计请求数量
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
    // 如果request或者response为空就不统计metric
    if (request_ == nullptr || response_ == nullptr)
        return;

    // 可以根据response中的返回值来统计此次请求的处理结果
    ChunkServerMetric* metric = ChunkServerMetric::GetInstance();
    bool hasError = false;
    uint64_t latencyUs =
        common::TimeUtility::GetTimeofDayUs() - receivedTimeUs_;
    switch (request_->optype()) {
        case CHUNK_OP_TYPE::CHUNK_OP_READ: {
            // 如果是read请求，返回CHUNK_OP_STATUS_CHUNK_NOTEXIST也认为是正确的
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

/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: 2022-03-23
 * Author: Jingli Chen (Wine93)
 */

#include "absl/cleanup/cleanup.h"
#include "curvefs/src/common/rpc_stream.h"
#include "curvefs/src/metaserver/metastore.h"
#include "curvefs/src/metaserver/copyset/meta_operator.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

using ::curvefs::common::StreamSender;
using ::curvefs::common::StreamOptions;
using ::curvefs::common::StreamConnection;

// NOTE: now we need struct `brpc::Controller` for sending data by stream,
// so we redefine OnApply() and OnApplyFromLog() instead of using macro.
// It may not be an elegant implementation, can you provide a better idea?
void GetOrModifyS3ChunkInfoOperator::OnApply(int64_t index,
                                             google::protobuf::Closure* done,
                                             uint64_t startTimeUs) {
    MetaStatusCode rc;
    auto request = static_cast<const GetOrModifyS3ChunkInfoRequest*>(request_);
    auto response = static_cast<GetOrModifyS3ChunkInfoResponse*>(response_);
    bool streaming = request->returns3chunkinfomap();
    auto metastore = node_->GetMetaStore();
    std::shared_ptr<StreamConnection> connection = nullptr;
    std::shared_ptr<Iterator> iterator;
    auto streamSender = metastore->GetStreamSender();
    auto defer = absl::MakeCleanup([&]() {
        streamSender->Close(connection);
    });
    {
        brpc::ClosureGuard doneGuard(done);

        rc = metastore->GetOrModifyS3ChunkInfo(request, response, &iterator);
        if (rc == MetaStatusCode::OK) {
            node_->UpdateAppliedIndex(index);
            response->set_appliedindex(
                std::max<uint64_t>(index, node_->GetAppliedIndex()));
            node_->GetMetric()->OnOperatorComplete(
                OperatorType::GetOrModifyS3ChunkInfo,
                TimeUtility::GetTimeofDayUs() - startTimeUs, true);
        } else {
            node_->GetMetric()->OnOperatorComplete(
                OperatorType::GetOrModifyS3ChunkInfo,
                TimeUtility::GetTimeofDayUs() - startTimeUs, false);
        }

        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_);
        if (rc != MetaStatusCode::OK || !streaming) {
            return;
        }

        // rc == MetaStatusCode::OK && streaming
        connection = streamSender->Open(cntl, nullptr, StreamOptions());
        if (nullptr == connection) {
            LOG(ERROR) << "Open stream connection failed";
            response->set_statuscode(MetaStatusCode::RPC_STREAM_ERROR);
            return;
        }
    }

    rc = metastore->SendS3ChunkInfoByStream(connection, iterator);
    if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "SendS3ChunkInfoByStream() failed";
    }
}

void GetOrModifyS3ChunkInfoOperator::OnApplyFromLog(uint64_t startTimeUs) {
    std::unique_ptr<GetOrModifyS3ChunkInfoOperator> selfGuard(this);
    GetOrModifyS3ChunkInfoResponse response;
    std::shared_ptr<Iterator> iterator;
    auto status = node_->GetMetaStore()->GetOrModifyS3ChunkInfo(
        static_cast<const GetOrModifyS3ChunkInfoRequest*>(request_),
        &response,
        &iterator);
    node_->GetMetric()->OnOperatorComplete(
        OperatorType::GetOrModifyS3ChunkInfo,
        TimeUtility::GetTimeofDayUs() - startTimeUs,
        status == MetaStatusCode::OK);
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

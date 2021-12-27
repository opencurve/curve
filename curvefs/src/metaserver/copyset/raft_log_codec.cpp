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
 * Project: curve
 * Date: Mon Aug  9 15:15:23 CST 2021
 * Author: wuhanqing
 */

#include "curvefs/src/metaserver/copyset/raft_log_codec.h"

#include <butil/sys_byteorder.h>
#include <glog/logging.h>

#include <memory>
#include <type_traits>

#include "curvefs/proto/metaserver.pb.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

namespace {

template <typename MetaOperatorT, typename RequestT>
inline std::unique_ptr<MetaOperator> ParseFromRaftLog(CopysetNode* node,
                                                      OperatorType type,
                                                      const butil::IOBuf& log) {
    butil::IOBufAsZeroCopyInputStream wrapper(log);
    auto request = absl::make_unique<RequestT>();

    bool success = request->ParseFromZeroCopyStream(&wrapper);
    if (success) {
        return absl::make_unique<MetaOperatorT>(node, request.release());
    }

    LOG(ERROR) << "Fail to parse request from raft log, type: "
               << OperatorTypeName(type);
    return nullptr;
}

}  // namespace

bool RaftLogCodec::Encode(OperatorType type,
                          const google::protobuf::Message* request,
                          butil::IOBuf* log) {
    static_assert(
        std::is_same<std::underlying_type<OperatorType>::type, uint32_t>::value,
        "OperatorType underlying type must be uint32_t");

    // 1. append operator type
    const uint32_t networkType =
        butil::HostToNet32(static_cast<uint32_t>(type));
    log->append(&networkType, sizeof(networkType));

    // 2. append request length
    // serialize will fail when request's size larger than INT_MAX, check this
    // manullay because `request->ByteSize()`'s behaviour is affected by NDEBUG
    const uint64_t requestSize = request->ByteSizeLong();
    if (CURVE_UNLIKELY(requestSize > INT_MAX)) {
        LOG(ERROR) << "Request's size is too large, type: "
                   << OperatorTypeName(type) << ", size: " << requestSize;
        return false;
    }

    const uint32_t networkRequestSize =
        butil::HostToNet32(static_cast<uint32_t>(requestSize));
    log->append(&networkRequestSize, sizeof(networkRequestSize));

    // 3. append serialized request
    butil::IOBufAsZeroCopyOutputStream wrapper(log);
    if (request->SerializeToZeroCopyStream(&wrapper)) {
        return true;
    }

    LOG(ERROR) << "Fail to serialize request, type: " << OperatorTypeName(type)
               << ", request: " << request->ShortDebugString();

    return false;
}

std::unique_ptr<MetaOperator> RaftLogCodec::Decode(CopysetNode* node,
                                                   butil::IOBuf log) {
    uint32_t logtype;
    log.cutn(&logtype, kOperatorTypeSize);
    logtype = butil::NetToHost32(logtype);

    uint32_t metaSize;
    log.cutn(&metaSize, sizeof(metaSize));
    metaSize = butil::NetToHost32(metaSize);

    butil::IOBuf meta;
    log.cutn(&meta, metaSize);

    OperatorType type = static_cast<OperatorType>(logtype);

    switch (type) {
        case OperatorType::GetDentry:
            return ParseFromRaftLog<GetDentryOperator, GetDentryRequest>(
                node, type, meta);
        case OperatorType::ListDentry:
            return ParseFromRaftLog<ListDentryOperator, ListDentryRequest>(
                node, type, meta);
        case OperatorType::CreateDentry:
            return ParseFromRaftLog<CreateDentryOperator, CreateDentryRequest>(
                node, type, meta);
        case OperatorType::DeleteDentry:
            return ParseFromRaftLog<DeleteDentryOperator, DeleteDentryRequest>(
                node, type, meta);
        case OperatorType::GetInode:
            return ParseFromRaftLog<GetInodeOperator, GetInodeRequest>(
                node, type, meta);
        case OperatorType::CreateInode:
            return ParseFromRaftLog<CreateInodeOperator, CreateInodeRequest>(
                node, type, meta);
        case OperatorType::UpdateInode:
            return ParseFromRaftLog<UpdateInodeOperator, UpdateInodeRequest>(
                node, type, meta);
        case OperatorType::DeleteInode:
            return ParseFromRaftLog<DeleteInodeOperator, DeleteInodeRequest>(
                node, type, meta);
        case OperatorType::CreateRootInode:
            return ParseFromRaftLog<CreateRootInodeOperator,
                                    CreateRootInodeRequest>(node, type, meta);
        case OperatorType::CreatePartition:
            return ParseFromRaftLog<CreatePartitionOperator,
                                    CreatePartitionRequest>(node, type, meta);
        case OperatorType::DeletePartition:
            return ParseFromRaftLog<DeletePartitionOperator,
                                    DeletePartitionRequest>(node, type, meta);
        case OperatorType::PrepareRenameTx:
            return ParseFromRaftLog<PrepareRenameTxOperator,
                                    PrepareRenameTxRequest>(node, type, meta);
        case OperatorType::AppendS3ChunkInfo:
            return ParseFromRaftLog<AppendS3ChunkInfoOperator,
                                    AppendS3ChunkInfoRequest>(node, type, meta);
        default:
            LOG(ERROR) << "unexpected type: " << static_cast<uint32_t>(type);
            return nullptr;
    }
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

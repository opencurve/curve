/*
 * Project: curve
 * Created Date: Fri Mar 08 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include <thread> //NOLINT
#include <chrono> //NOLINT

#include "src/mds/chunkserverclient/copyset_client.h"

using ::curve::mds::topology::CopySetInfo;
using ::curve::mds::topology::CopySetKey;
using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::ChunkServerIdType;
using ::curve::mds::topology::UNINTIALIZE_ID;

namespace curve {
namespace mds {
namespace chunkserverclient {
int CopysetClient::DeleteChunkSnapshotOrCorrectSn(LogicalPoolID logicalPoolId,
    CopysetID copysetId,
    ChunkID chunkId,
    uint64_t correctedSn) {
    int ret = kMdsFail;
    CopySetInfo copyset;
    if (true != topo_->GetCopySet(
        CopySetKey(logicalPoolId, copysetId),
        &copyset)) {
        LOG(ERROR) << "GetCopySet fail.";
        return kMdsFail;
    }

    ChunkServerIdType leaderId =
        copyset.GetLeader();

    if (leaderId != UNINTIALIZE_ID) {
        ret = chunkserverClient_->DeleteChunkSnapshotOrCorrectSn(
            leaderId, logicalPoolId, copysetId, chunkId, correctedSn);
        if (kMdsSuccess == ret) {
            return ret;
        }
    }

    uint32_t retry = 1;
    while ((retry < updateLeaderRetryTimes_) &&
           ((UNINTIALIZE_ID == leaderId) ||
            (kCsClientCSOffline == ret) ||
            (kRpcFail == ret) ||
            (kCsClientNotLeader == ret))) {
        std::this_thread::sleep_for(
                std::chrono::milliseconds(updateLeaderRetryIntervalMs_));
        ret = UpdateLeader(&copyset);
        if (ret < 0) {
            LOG(ERROR) << "UpdateLeader fail."
                       << " logicalPoolId = " << logicalPoolId
                       << ", copysetId = " << copysetId;
            break;
        }

        leaderId = copyset.GetLeader();

        if (leaderId != UNINTIALIZE_ID) {
            ret = chunkserverClient_->DeleteChunkSnapshotOrCorrectSn(
                leaderId, logicalPoolId, copysetId, chunkId, correctedSn);
            if (kMdsSuccess == ret) {
                break;
            }
        } else {
            LOG(ERROR) << "UpdateLeader success, but leaderId is uninit.";
            return kMdsFail;
        }
        retry++;
    }
    return ret;
}

int CopysetClient::DeleteChunk(LogicalPoolID logicalPoolId,
                                    CopysetID copysetId,
                                    ChunkID chunkId,
                                    uint64_t sn) {
    int ret = kMdsFail;
    CopySetInfo copyset;
    if (true != topo_->GetCopySet(
        CopySetKey(logicalPoolId, copysetId),
        &copyset)) {
        LOG(ERROR) << "GetCopySet fail.";
        return kMdsFail;
    }

    ChunkServerIdType leaderId =
        copyset.GetLeader();

    if (leaderId != UNINTIALIZE_ID) {
        ret = chunkserverClient_->DeleteChunk(
            leaderId, logicalPoolId, copysetId, chunkId, sn);
        if (kMdsSuccess == ret) {
            return ret;
        }
    }

    // delete Chunk在kCsClientCSOffline、kRpcFail、kCsClientNotLeader
    // 这三种返回值时需要进行重试
    uint32_t retry = 1;
    while ((retry < updateLeaderRetryTimes_) &&
           ((UNINTIALIZE_ID == leaderId) ||
            (kCsClientCSOffline == ret) ||
            (kRpcFail == ret) ||
            (kCsClientNotLeader == ret))) {
        std::this_thread::sleep_for(
                std::chrono::milliseconds(updateLeaderRetryIntervalMs_));
        ret = UpdateLeader(&copyset);
        if (ret < 0) {
            LOG(ERROR) << "UpdateLeader fail."
                       << " logicalPoolId = " << logicalPoolId
                       << ", copysetId = " << copysetId;
            break;
        }

        leaderId = copyset.GetLeader();

        if (leaderId != UNINTIALIZE_ID) {
            ret = chunkserverClient_->DeleteChunk(
                leaderId, logicalPoolId, copysetId, chunkId, sn);
            if (kMdsSuccess == ret) {
                break;
            }
        } else {
            LOG(ERROR) << "UpdateLeader success, but leaderId is uninit.";
            return kMdsFail;
        }
        retry++;
    }
    return ret;
}

int CopysetClient::UpdateLeader(CopySetInfo *copyset) {
    LogicalPoolID logicalPoolId = copyset->GetLogicalPoolId();
    CopysetID copysetId = copyset->GetId();
    int ret = kMdsFail;
    ChunkServerIdType leader;
    bool success = false;
    for (auto &csId : copyset->GetCopySetMembers()) {
        ret = chunkserverClient_->GetLeader(
            csId, logicalPoolId, copysetId, &leader);
        if (kMdsSuccess == ret) {
            success = true;
            break;
        }
    }
    if (success) {
        copyset->SetLeader(leader);
        ret = kMdsSuccess;
    }
    return ret;
}

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve

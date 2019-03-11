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


namespace curve {
namespace mds {
namespace chunkserverclient {

int CopysetClient::DeleteSnapShotChunk(LogicalPoolID logicalPoolId,
    CopysetID copysetId,
    ChunkID chunkId,
    uint64_t sn) {
    CopySetInfo copyset;
    if (true != topo_->GetCopySet(
        CopySetKey(logicalPoolId, copysetId),
        &copyset)) {
        return kMdsFail;
    }

    ChunkServerIdType leaderId =
        copyset.GetLeader();

    int ret = chunkserverClient_->DeleteChunkSnapshot(
        leaderId, logicalPoolId, copysetId, chunkId, sn);
    if (kMdsSuccess == ret) {
        return ret;
    }

    uint32_t retry = 1;
    while ((retry < kUpdateLeaderRetryTime) &&
           ((kCsClientCSOffline == ret) ||
            (kRpcFail == ret) ||
            (kCsClientNotLeader == ret))) {
        std::this_thread::sleep_for(
                std::chrono::milliseconds(kUpdateLeaderRetryIntervalMs));
        ret = UpdateLeader(&copyset);
        if (ret < 0) {
            LOG(ERROR) << "UpdateLeader fail."
                       << " logicalPoolId = " << logicalPoolId
                       << ", copysetId = " << copysetId;
            break;
        }

        leaderId = copyset.GetLeader();

        ret = chunkserverClient_->DeleteChunkSnapshot(
            leaderId, logicalPoolId, copysetId, chunkId, sn);
        if (kMdsSuccess == ret) {
            break;
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
        // TODO(xuchaojie): update topo copyset, 这里需要同步, 防止更新成旧的copyset。 NOLINT
        ret = kMdsSuccess;
    }
    return ret;
}

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve

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
 * Created Date: Fri Mar 08 2019
 * Author: xuchaojie
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

    uint32_t retry = 0;
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
        LOG(INFO) << "UpdateLeader success, new leaderId = " << leaderId;

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

int CopysetClient::DeleteChunkSnapshot(LogicalPoolID logicalPoolId,
    CopysetID copysetId,
    ChunkID chunkId,
    uint64_t snapSn,
    const std::vector<uint64_t>& snaps) {
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
        ret = chunkserverClient_->DeleteChunkSnapshot(
            leaderId, logicalPoolId, copysetId, chunkId, snapSn, snaps);
        if (kMdsSuccess == ret) {
            return ret;
        }
    }

    uint32_t retry = 0;
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
        LOG(INFO) << "UpdateLeader success, new leaderId = " << leaderId;

        if (leaderId != UNINTIALIZE_ID) {
            ret = chunkserverClient_->DeleteChunkSnapshot(
                leaderId, logicalPoolId, copysetId, chunkId, snapSn, snaps);
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

    // delete Chunk needs to retry when kCsClientCSOffline
    // or kRpcFail or kCsClientNotLeader returned
    uint32_t retry = 0;
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
        LOG(INFO) << "UpdateLeader success, new leaderId = " << leaderId;

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
        // 判断leader是否在copyset中，否则需要更新copyset
        if (copyset->HasMember(leader)) {
            copyset->SetLeader(leader);
            ret = kMdsSuccess;
        } else {
            ret = kLeaderNotInCopyset;
        }
    }
    return ret;
}


struct ChunkServerFlattenChunkClosure : public ChunkServerClientClosure {
 public:
    explicit ChunkServerFlattenChunkClosure(
            CopysetClient *client,
            CopysetClientClosure* done)
        : client_(client), done_(done) {}
    virtual ~ChunkServerFlattenChunkClosure() {}

    void Run() override {
        std::unique_ptr<ChunkServerFlattenChunkClosure> selfGuard(this);
        brpc::ClosureGuard doneGuard(done_);
        int ret = GetErrCode();
        done_->SetErrCode(ret);
        if (ret == kMdsSuccess) {
            return;
        }

        while ((curTry < updateLeaderRetryTimes) &&
               ((UNINTIALIZE_ID == leaderId) ||
                (kCsClientCSOffline == ret) ||
                (kRpcFail == ret) ||
                (kCsClientNotLeader == ret))) {
            std::this_thread::sleep_for(
                    std::chrono::milliseconds(updateLeaderRetryIntervalMs));
            curTry++;
            ret = client_->UpdateLeader(&copyset);
            if (ret < 0) {
                if (kLeaderNotInCopyset == ret) {
                    // refresh copyset
                    if (true != client_->topo_->GetCopySet(
                        CopySetKey(ctx->logicalPoolId, ctx->copysetId),
                        &copyset)) {
                        LOG(ERROR) << "GetCopySet fail.";
                        ret = kMdsFail; break;
                    }
                } else {
                    LOG(WARNING) << "UpdateLeader fail, ret = " << ret
                               << ", logicalPoolId = " << ctx->logicalPoolId
                               << ", copysetId = " << ctx->copysetId
                               << ", curTry = " << curTry;
                    continue;
                }
            }

            leaderId = copyset.GetLeader();
            LOG(INFO) << "UpdateLeader success, new leaderId = " << leaderId;

            if (leaderId != UNINTIALIZE_ID) {
                ret = client_->chunkserverClient_->FlattenChunk(
                    leaderId, ctx, selfGuard.get());
                if (kMdsSuccess == ret) {
                    selfGuard.release();
                    doneGuard.release();
                    return;
                }
            } else {
                continue;
            }
        }
        done_->SetErrCode(ret);
    }

 public:
    uint32_t curTry;
    uint32_t updateLeaderRetryTimes;
    uint32_t updateLeaderRetryIntervalMs;
    std::shared_ptr<FlattenChunkContext> ctx;

    ChunkServerIdType leaderId;
    CopySetInfo copyset;

 private:
    CopysetClient *client_;
    CopysetClientClosure* done_;
};

int CopysetClient::FlattenChunk(
    const std::shared_ptr<FlattenChunkContext> &ctx, 
    CopysetClientClosure* done) {
    LogicalPoolID logicalPoolId = ctx->logicalPoolId;
    CopysetID copysetId = ctx->copysetId;

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

    std::unique_ptr<ChunkServerFlattenChunkClosure> csvClosure(
        new ChunkServerFlattenChunkClosure(this, done));
    csvClosure->curTry = 0;
    csvClosure->updateLeaderRetryTimes = updateLeaderRetryTimes_;
    csvClosure->updateLeaderRetryIntervalMs = updateLeaderRetryIntervalMs_;
    csvClosure->leaderId = leaderId;
    csvClosure->copyset = copyset;
    csvClosure->ctx = ctx;

    if (leaderId != UNINTIALIZE_ID) {
        csvClosure->curTry++;
        ret = chunkserverClient_->FlattenChunk(
            leaderId, ctx, csvClosure.get());
        if (kMdsSuccess == ret) {
            csvClosure.release();
            return ret;
        }
    }

    while ((csvClosure->curTry < csvClosure->updateLeaderRetryTimes) &&
           ((UNINTIALIZE_ID == leaderId) ||
            (kCsClientCSOffline == ret) ||
            (kRpcFail == ret) ||
            (kCsClientNotLeader == ret))) {
        std::this_thread::sleep_for(
                std::chrono::milliseconds(
                    csvClosure->updateLeaderRetryIntervalMs));
        csvClosure->curTry++;
        ret = UpdateLeader(&copyset);
        if (ret < 0) {
            if (kLeaderNotInCopyset == ret) {
                // refresh copyset
                if (true != topo_->GetCopySet(
                    CopySetKey(logicalPoolId, copysetId),
                    &copyset)) {
                    LOG(ERROR) << "GetCopySet fail.";
                    return kMdsFail;
                }
            } else {
                LOG(WARNING) << "UpdateLeader fail, ret = " << ret
                           << ", logicalPoolId = " << logicalPoolId
                           << ", copysetId = " << copysetId
                           << ", curTry = " << csvClosure->curTry;
                continue;
            }
        }

        leaderId = copyset.GetLeader();
        LOG(INFO) << "UpdateLeader success, new leaderId = " << leaderId;

        if (leaderId != UNINTIALIZE_ID) {
            ret = chunkserverClient_->FlattenChunk(
                leaderId, ctx, 
                csvClosure.get());
            if (kMdsSuccess == ret) {
                csvClosure.release();
                break;
            }
        } else {
            continue;
        }
    }
    return ret;
}


}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve

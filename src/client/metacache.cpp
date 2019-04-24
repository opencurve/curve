/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 2:06:35 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <bthread/bthread.h>

#include "proto/cli.pb.h"

#include "src/client/metacache.h"
#include "src/client/mds_client.h"
#include "src/client/client_common.h"
#include "src/common/concurrent/concurrent.h"

using curve::common::WriteLockGuard;
using curve::common::ReadLockGuard;
using curve::client::ClientConfig;

namespace curve {
namespace client {
MetaCache::MetaCache() {
}

MetaCache::~MetaCache() {
    chunkindex2idMap_.clear();
    chunkid2chunkInfoMap_.clear();
    lpcsid2CopsetInfoMap_.clear();
}

void MetaCache::Init(MetaCacheOption_t metaCacheOpt) {
    metacacheopt_ = metaCacheOpt;
}

MetaCacheErrorType MetaCache::GetChunkInfoByIndex(ChunkIndex chunkidx, ChunkIDInfo_t* chunxinfo ) {  // NOLINT
    ReadLockGuard rdlk(rwlock4ChunkInfo_);
    auto iter = chunkindex2idMap_.find(chunkidx);
    if (iter != chunkindex2idMap_.end()) {
        *chunxinfo = iter->second;
        return MetaCacheErrorType::OK;
    }
    return MetaCacheErrorType::CHUNKINFO_NOT_FOUND;
}

int MetaCache::GetLeader(LogicPoolID logicPoolId,
                        CopysetID copysetId,
                        ChunkServerID* serverId,
                        EndPoint* serverAddr,
                        bool refresh) {
    std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);

    CopysetInfo_t targetInfo;
    rwlock4CopysetInfo_.RDLock();
    auto iter = lpcsid2CopsetInfoMap_.find(mapkey);
    if (iter == lpcsid2CopsetInfoMap_.end()) {
        rwlock4CopysetInfo_.Unlock();
        LOG(ERROR) << "server list not exist, LogicPoolID = " << logicPoolId
                   << ", CopysetID = " << copysetId;
        return -1;
    }
    targetInfo = iter->second;
    rwlock4CopysetInfo_.Unlock();

    int ret = 0;
    if (refresh) {
        uint32_t retry = 0;
        while (retry++ < metacacheopt_.getLeaderRetry) {
            bthread_usleep(metacacheopt_.retryIntervalUs);
            ret = UpdateLeaderInternal(logicPoolId, copysetId, &targetInfo);
            if (ret != -1) {
                UpdateCopysetInfo(logicPoolId, copysetId, targetInfo);
                break;
            }
        }
    }

    if (ret == -1) {
        LOG(ERROR) << "get leader failed after retry!";
        return -1;
    }

    return targetInfo.GetLeaderInfo(serverId, serverAddr);
}

int MetaCache::UpdateLeaderInternal(LogicPoolID logicPoolId,
                                    CopysetID copysetId,
                                    CopysetInfo* toupdateCopyset) {
    ChunkServerAddr  leaderaddr;
    int ret = ServiceHelper::GetLeader(logicPoolId, copysetId,
                                      toupdateCopyset->csinfos_, &leaderaddr,
                                      toupdateCopyset->GetCurrentLeaderIndex());

    if (ret == -1) {
        LOG(ERROR) << "get leader failed!";
        return -1;
    }
    toupdateCopyset->ChangeLeaderID(leaderaddr);
    return 0;
}

CopysetInfo_t MetaCache::GetServerList(LogicPoolID logicPoolId,
                                        CopysetID copysetId) {
    std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);
    CopysetInfo_t ret;

    ReadLockGuard rdlk(rwlock4CopysetInfo_);
    auto iter = lpcsid2CopsetInfoMap_.find(mapkey);
    if (iter == lpcsid2CopsetInfoMap_.end()) {
        // it's impossible to get here
        return ret;
    }
    return iter->second;
}

/**
 * when copyset client find that the leader is redirect,
 * the copyset client will call UpdateLeader.
 * return the ChunkServerID to invoker
 */
int MetaCache::UpdateLeader(LogicPoolID logicPoolId,
                CopysetID copysetId,
                ChunkServerID* leaderId,
                const EndPoint &leaderAddr) {
    std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);

    ReadLockGuard rdlk(rwlock4CopysetInfo_);
    auto iter = lpcsid2CopsetInfoMap_.find(mapkey);
    if (iter == lpcsid2CopsetInfoMap_.end()) {
        // it's impossible to get here
        return -1;
    }

    return iter->second.UpdateLeaderAndGetChunkserverID(leaderId, leaderAddr);
}

void MetaCache::UpdateChunkInfoByIndex(ChunkIndex cindex, ChunkIDInfo_t cinfo) {  // NOLINT
    WriteLockGuard wrlk(rwlock4ChunkInfo_);
    chunkindex2idMap_[cindex] = cinfo;
}

void MetaCache::UpdateCopysetInfo(LogicPoolID logicPoolid,
                                        CopysetID copysetid,
                                        CopysetInfo_t csinfo) {
    auto key = LogicPoolCopysetID2Str(logicPoolid, copysetid);
    WriteLockGuard wrlk(rwlock4CopysetInfo_);
    lpcsid2CopsetInfoMap_[key] = csinfo;
}

void MetaCache::UpdateAppliedIndex(LogicPoolID logicPoolId,
                                CopysetID copysetId,
                                uint64_t appliedindex) {
    std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);

    WriteLockGuard wrlk(rwlock4CopysetInfo_);
    auto iter = lpcsid2CopsetInfoMap_.find(mapkey);
    if (iter == lpcsid2CopsetInfoMap_.end()) {
        return;
    }
    iter->second.UpdateAppliedIndex(appliedindex);
}

uint64_t MetaCache::GetAppliedIndex(LogicPoolID logicPoolId,
                                    CopysetID copysetId) {
    std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);

    ReadLockGuard rdlk(rwlock4CopysetInfo_);
    auto iter = lpcsid2CopsetInfoMap_.find(mapkey);
    if (iter == lpcsid2CopsetInfoMap_.end()) {
        return 0;
    }

    return iter->second.GetAppliedIndex();
}

void MetaCache::UpdateChunkInfoByID(ChunkID cid, ChunkIDInfo cidinfo) {
    WriteLockGuard wrlk(rwlock4chunkInfoMap_);
    chunkid2chunkInfoMap_[cid] = cidinfo;
}

MetaCacheErrorType MetaCache::GetChunkInfoByID(ChunkID chunkid,
                                ChunkIDInfo_t* chunkinfo) {
    ReadLockGuard rdlk(rwlock4chunkInfoMap_);
    auto iter = chunkid2chunkInfoMap_.find(chunkid);
    if (iter != chunkid2chunkInfoMap_.end()) {
        *chunkinfo = iter->second;
        return MetaCacheErrorType::OK;
    }
    return MetaCacheErrorType::CHUNKINFO_NOT_FOUND;
}

std::string MetaCache::LogicPoolCopysetChunkID2Str(LogicPoolID lpid,
                                                    CopysetID csid,
                                                    ChunkID chunkid) {
    return std::to_string(lpid).append("_")
                                .append(std::to_string(csid))
                                .append("_")
                                .append(std::to_string(chunkid));
}
std::string MetaCache::LogicPoolCopysetID2Str(LogicPoolID lpid,
                                             CopysetID csid) {
    return std::to_string(lpid).append("_").append(std::to_string(csid));
}
}   // namespace client
}   // namespace curve

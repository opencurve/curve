/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 2:06:35 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "proto/cli.pb.h"

#include "src/client/metacache.h"
#include "src/client/mds_client.h"
#include "src/client/client_common.h"

using curve::client::ClientConfig;

namespace curve {
namespace client {
MetaCache::MetaCache() {
}

MetaCache::~MetaCache() {
    chunkindex2idMap_.clear();
    chunkid2chunkInfoMap_.clear();
    lpcsid2serverlistMap_.clear();
}

void MetaCache::Init(MetaCacheOption_t metaCacheOpt) {
    metacacheopt_ = metaCacheOpt;
}

MetaCacheErrorType MetaCache::GetChunkInfoByIndex(ChunkIndex chunkidx, ChunkIDInfo_t* chunxinfo ) {  // NOLINT
    rwlock4ChunkInfo_.RDLock();
    auto iter = chunkindex2idMap_.find(chunkidx);
    if (iter != chunkindex2idMap_.end()) {
        rwlock4ChunkInfo_.Unlock();
        *chunxinfo = iter->second;
        return MetaCacheErrorType::OK;
    }
    rwlock4ChunkInfo_.Unlock();
    return MetaCacheErrorType::CHUNKINFO_NOT_FOUND;
}

/**
 * 1. check the logicPoolId and CopysetID assosiate serverlist exists or not
 * 2. server list should exist, because GetOrAllocateSegment will 
 *    get serverlist imediately
 * 3. check exists again, if not exists, return failed
 * 4. if exists, check if need refresh
 * 5. if need refresh, we invoke chunkserver cli getleader service
 * 6. if no need refresh, just return the leader id and server addr
 */
int MetaCache::GetLeader(LogicPoolID logicPoolId,
                        CopysetID copysetId,
                        ChunkServerID* serverId,
                        EndPoint* serverAddr,
                        bool refresh) {
    std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);

    rwlock4CopysetInfo_.RDLock();
    auto iter = lpcsid2serverlistMap_.find(mapkey);
    if (iter == lpcsid2serverlistMap_.end()) {
        rwlock4CopysetInfo_.Unlock();
        LOG(ERROR) << "server list not exist, LogicPoolID = " << logicPoolId
                   << ", CopysetID = " << copysetId;
        return -1;
    }
    rwlock4CopysetInfo_.Unlock();

    auto getleader = [&]() ->int {
        Configuration cfg;
        for (auto it : iter->second.csinfos_) {
            cfg.add_peer(it.peerid_);
        }
        PeerId  leaderid;
        int ret = ServiceHelper::GetLeader(logicPoolId,
                                            copysetId,
                                            cfg,
                                            &leaderid);
        if (ret == -1) {
            LOG(ERROR) << "get leader failed!";
            return -1;
        }
        iter->second.ChangeLeaderID(leaderid);
        return 0;
    };

    int ret = 0;
    if (refresh) {
        uint32_t retry = 0;
        while (retry++ < metacacheopt_.getLeaderRetry) {
            usleep(metacacheopt_.retryIntervalUs);
            ret = getleader();
            if (ret != -1) {
                break;
            }
        }
    }

    if (ret == -1) {
        LOG(ERROR) << "get leader failed after retry!";
        return -1;
    }
    return iter->second.GetLeaderInfo(serverId, serverAddr);
}

CopysetInfo_t MetaCache::GetServerList(LogicPoolID logicPoolId,
                                        CopysetID copysetId) {
    std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);
    CopysetInfo_t ret;
    rwlock4CopysetInfo_.RDLock();
    auto iter = lpcsid2serverlistMap_.find(mapkey);
    if (iter == lpcsid2serverlistMap_.end()) {
        // it's impossible to get here
        rwlock4CopysetInfo_.Unlock();
        return ret;
    }
    rwlock4CopysetInfo_.Unlock();
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

    int ret = 0;
    rwlock4CopysetInfo_.RDLock();
    auto iter = lpcsid2serverlistMap_.find(mapkey);
    if (iter == lpcsid2serverlistMap_.end()) {
        // it's impossible to get here
        rwlock4CopysetInfo_.Unlock();
        return -1;
    }
    ret = iter->second.UpdateLeaderAndGetChunkserverID(leaderId, leaderAddr); // NOLINT
    rwlock4CopysetInfo_.Unlock();
    return ret;
}

void MetaCache::UpdateChunkInfoByIndex(ChunkIndex cindex, ChunkIDInfo_t cinfo) {  // NOLINT
    rwlock4ChunkInfo_.WRLock();
    chunkindex2idMap_[cindex] = cinfo;
    rwlock4ChunkInfo_.Unlock();
}

void MetaCache::UpdateCopysetInfo(LogicPoolID logicPoolid,
                                        CopysetID copysetid,
                                        CopysetInfo_t cslist) {
    auto key = LogicPoolCopysetID2Str(logicPoolid, copysetid);
    rwlock4CopysetInfo_.WRLock();
    lpcsid2serverlistMap_[key] = cslist;
    rwlock4CopysetInfo_.Unlock();
}

void MetaCache::UpdateAppliedIndex(LogicPoolID logicPoolId,
                                CopysetID copysetId,
                                uint64_t appliedindex) {
    std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);
    rwlock4CopysetInfo_.RDLock();
    auto iter = lpcsid2serverlistMap_.find(mapkey);
    if (iter == lpcsid2serverlistMap_.end()) {
        rwlock4CopysetInfo_.Unlock();
        return;
    }
    iter->second.UpdateAppliedIndex(appliedindex);
    rwlock4CopysetInfo_.Unlock();
}

uint64_t MetaCache::GetAppliedIndex(LogicPoolID logicPoolId,
                                    CopysetID copysetId) {
    std::string mapkey = LogicPoolCopysetID2Str(logicPoolId, copysetId);
    rwlock4CopysetInfo_.RDLock();
    auto iter = lpcsid2serverlistMap_.find(mapkey);
    if (iter == lpcsid2serverlistMap_.end()) {
        rwlock4CopysetInfo_.Unlock();
        return 0;
    }
    uint64_t appliedindex = iter->second.GetAppliedIndex();
    rwlock4CopysetInfo_.Unlock();
    return appliedindex;
}

void MetaCache::UpdateChunkInfoByID(LogicPoolID lpid,
                                    CopysetID cpid,
                                    ChunkID cid) {
    auto key = LogicPoolCopysetChunkID2Str(lpid, cpid, cid);
    rwlock4chunkInfoMap_.WRLock();
    chunkid2chunkInfoMap_[key] = ChunkIDInfo(cid, lpid, cpid);
    rwlock4chunkInfoMap_.Unlock();
}

MetaCacheErrorType MetaCache::GetChunkInfoByID(LogicPoolID lpid,
                                CopysetID cpid,
                                ChunkID chunkid,
                                ChunkIDInfo_t* chunkinfo) {
    auto key = LogicPoolCopysetChunkID2Str(lpid, cpid, chunkid);
    rwlock4chunkInfoMap_.RDLock();
    auto iter = chunkid2chunkInfoMap_.find(key);
    if (iter != chunkid2chunkInfoMap_.end()) {
        rwlock4chunkInfoMap_.Unlock();
        *chunkinfo = iter->second;
        return MetaCacheErrorType::OK;
    }
    rwlock4chunkInfoMap_.Unlock();
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

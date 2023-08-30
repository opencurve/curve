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
 * File Created: Tuesday, 25th September 2018 2:06:35 pm
 * Author: tongguangxun
 */
#include <glog/logging.h>

#include <bthread/bthread.h>

#include <utility>
#include <vector>
#include <algorithm>

#include "proto/cli.pb.h"

#include "src/client/metacache.h"
#include "src/client/mds_client.h"
#include "src/client/client_common.h"
#include "src/common/concurrent/concurrent.h"

namespace curve {
namespace client {

using curve::common::WriteLockGuard;
using curve::common::ReadLockGuard;
using curve::client::ClientConfig;

void MetaCache::Init(const MetaCacheOption& metaCacheOpt,
                     MDSClient* mdsclient) {
    mdsclient_ = mdsclient;
    metacacheopt_ = metaCacheOpt;
    LOG(INFO) << "metacache init success, get leader retry times = "
              << metacacheopt_.metacacheGetLeaderRetry
              << ", get leader retry interval us = "
              << metacacheopt_.metacacheRPCRetryIntervalUS
              << ", get leader rpc time out ms = "
              << metacacheopt_.metacacheGetLeaderRPCTimeOutMS;

    unstableHelper_.Init(metacacheopt_.chunkserverUnstableOption);
}

MetaCacheErrorType MetaCache::GetChunkInfoByIndex(ChunkIndex chunkidx,
                                                  ChunkIDInfo* chunxinfo) {
    ReadLockGuard rdlk(rwlock4ChunkInfo_);
    auto iter = chunkindex2idMap_.find(chunkidx);
    if (iter != chunkindex2idMap_.end()) {
        *chunxinfo = iter->second;
        return MetaCacheErrorType::OK;
    }
    return MetaCacheErrorType::CHUNKINFO_NOT_FOUND;
}

void MetaCache::UpdateChunkInfoByIndex(ChunkIndex cindex,
                                       const ChunkIDInfo& cinfo) {
    WriteLockGuard wrlk(rwlock4ChunkInfo_);
    chunkindex2idMap_[cindex] = cinfo;
}

bool MetaCache::IsLeaderMayChange(LogicPoolID logicPoolId,
                                  CopysetID copysetId) {
    rwlock4ChunkInfo_.RDLock();
    auto iter = lpcsid2CopsetInfoMap_.find(
        CalcLogicPoolCopysetID(logicPoolId, copysetId));
    if (iter == lpcsid2CopsetInfoMap_.end()) {
        rwlock4ChunkInfo_.Unlock();
        return false;
    }

    bool flag = iter->second.LeaderMayChange();
    rwlock4ChunkInfo_.Unlock();
    return flag;
}

int MetaCache::GetLeader(LogicPoolID logicPoolId,
                         CopysetID copysetId,
                         ChunkServerID* serverId,
                         EndPoint* serverAddr,
                         bool refresh,
                         FileMetric* fm) {
    const auto key = CalcLogicPoolCopysetID(logicPoolId, copysetId);

    CopysetInfo<ChunkServerID> targetInfo;
    rwlock4CopysetInfo_.RDLock();
    auto iter = lpcsid2CopsetInfoMap_.find(key);
    if (iter == lpcsid2CopsetInfoMap_.end()) {
        rwlock4CopysetInfo_.Unlock();
        LOG(ERROR) << "server list not exist, LogicPoolID = " << logicPoolId
                   << ", CopysetID = " << copysetId;
        return -1;
    }
    targetInfo = iter->second;
    rwlock4CopysetInfo_.Unlock();

    int ret = 0;
    if (refresh || targetInfo.LeaderMayChange()) {
        uint32_t retry = 0;
        while (retry++ < metacacheopt_.metacacheGetLeaderRetry) {
            ret = UpdateLeaderInternal(logicPoolId, copysetId, &targetInfo, fm);
            if (ret != -1) {
                targetInfo.ResetSetLeaderUnstableFlag();
                UpdateCopysetInfo(logicPoolId, copysetId, targetInfo);
                break;
            }

            LOG(INFO) << "refresh leader from chunkserver failed, "
                      << "get copyset chunkserver list from mds, "
                      << "logicpool id = " << logicPoolId
                      << ", copyset id = " << copysetId;

            //The retry failed. At this point, it is necessary to retrieve the latest copyset information from mds again
            ret = UpdateCopysetInfoFromMDS(logicPoolId, copysetId);
            if (ret == 0) {
                continue;
            }

            bthread_usleep(metacacheopt_.metacacheRPCRetryIntervalUS);
        }
    }

    if (ret == -1) {
        LOG(WARNING) << "get leader failed after retry!"
            << ", copyset id = " << copysetId
            << ", logicpool id = " << logicPoolId;
        return -1;
    }

    return targetInfo.GetLeaderInfo(serverId, serverAddr);
}

int MetaCache::UpdateLeaderInternal(LogicPoolID logicPoolId,
                                    CopysetID copysetId,
                                    CopysetInfo<ChunkServerID>* toupdateCopyset,
                                    FileMetric* fm) {
    ChunkServerID csid = 0;
    PeerAddr  leaderaddr;
    GetLeaderRpcOption rpcOption(metacacheopt_.metacacheGetLeaderRPCTimeOutMS);
    GetLeaderInfo getLeaderInfo(logicPoolId,
                        copysetId, toupdateCopyset->csinfos_,
                        toupdateCopyset->GetCurrentLeaderIndex(),
                        rpcOption);
    int ret = ServiceHelper::GetLeader(
        getLeaderInfo, &leaderaddr, &csid, fm);

    if (ret == -1) {
        LOG(WARNING) << "get leader failed!"
            << ", copyset id = " << copysetId
            << ", logicpool id = " << logicPoolId;
        return -1;
    }

    ret = toupdateCopyset->UpdateLeaderInfo(leaderaddr);

    //If the update fails, it indicates that the leader address is not in the current configuration group. Obtain chunkserver information from MDS
    if (ret == -1 && !leaderaddr.IsEmpty()) {
        CopysetPeerInfo<ChunkServerID> csInfo;
        ret = mdsclient_->GetChunkServerInfo(leaderaddr, &csInfo);

        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "get chunkserver id from mds failed, addr = "
                       << leaderaddr.ToString();
            return -1;
        }

        UpdateCopysetInfoIfMatchCurrentLeader(
            logicPoolId, copysetId, leaderaddr);
        *toupdateCopyset = GetCopysetinfo(logicPoolId, copysetId);
        ret = toupdateCopyset->UpdateLeaderInfo(leaderaddr, csInfo);
    }

    return ret;
}

int MetaCache::UpdateCopysetInfoFromMDS(LogicPoolID logicPoolId,
                                        CopysetID copysetId) {
    std::vector<CopysetInfo<ChunkServerID>> copysetInfos;

    int ret =
        mdsclient_->GetServerList(logicPoolId, {copysetId}, &copysetInfos);

    if (copysetInfos.empty()) {
        LOG(WARNING) << "Get copyset server list from mds return empty server "
                        "list, ret = "
                     << ret << ", logicpool id = " << logicPoolId
                     << ", copyset id = " << copysetId;
        return -1;
    }

    //Update chunkserverid to copyset mapping relationship
    UpdateChunkserverCopysetInfo(logicPoolId, copysetInfos[0]);
    //Update the mapping of logicpool and copysetid to copysetinfo
    UpdateCopysetInfo(logicPoolId, copysetId, copysetInfos[0]);

    return 0;
}

void MetaCache::UpdateCopysetInfoIfMatchCurrentLeader(
    LogicPoolID logicPoolId,
    CopysetID copysetId,
    const PeerAddr& leaderAddr) {
    std::vector<CopysetInfo<ChunkServerID>> copysetInfos;
    (void)mdsclient_->GetServerList(logicPoolId, {copysetId}, &copysetInfos);

    bool needUpdate = (!copysetInfos.empty()) &&
                      (copysetInfos[0].HasPeerInCopyset(leaderAddr));
    if (needUpdate) {
        LOG(INFO) << "Update copyset info"
                  << ", logicpool id = " << logicPoolId
                  << ", copyset id = " << copysetId
                  << ", current leader = " << leaderAddr.ToString();

        //Update the mapping relationship between chunkserverid and copyset
        UpdateChunkserverCopysetInfo(logicPoolId, copysetInfos[0]);
        //Update the mapping of logicpool and copysetid to copysetinfo
        UpdateCopysetInfo(logicPoolId, copysetId, copysetInfos[0]);
    }
}

CopysetInfo<ChunkServerID> MetaCache::GetServerList(LogicPoolID logicPoolId,
                                     CopysetID copysetId) {
    const auto key = CalcLogicPoolCopysetID(logicPoolId, copysetId);
    CopysetInfo<ChunkServerID> ret;

    ReadLockGuard rdlk(rwlock4CopysetInfo_);
    auto iter = lpcsid2CopsetInfoMap_.find(key);
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
                            const EndPoint& leaderAddr) {
    const auto key = CalcLogicPoolCopysetID(logicPoolId, copysetId);

    ReadLockGuard rdlk(rwlock4CopysetInfo_);
    auto iter = lpcsid2CopsetInfoMap_.find(key);
    if (iter == lpcsid2CopsetInfoMap_.end()) {
        // it's impossible to get here
        return -1;
    }

    PeerAddr csAddr(leaderAddr);
    return iter->second.UpdateLeaderInfo(csAddr);
}

void MetaCache::UpdateCopysetInfo(LogicPoolID logicPoolid, CopysetID copysetid,
                                  const CopysetInfo<ChunkServerID>& csinfo) {
    const auto key = CalcLogicPoolCopysetID(logicPoolid, copysetid);
    WriteLockGuard wrlk(rwlock4CopysetInfo_);
    lpcsid2CopsetInfoMap_[key] = csinfo;
}

void MetaCache::UpdateChunkInfoByID(ChunkID cid, const ChunkIDInfo& cidinfo) {
    WriteLockGuard wrlk(rwlock4chunkInfoMap_);
    chunkid2chunkInfoMap_[cid] = cidinfo;
}

int MetaCache::SetServerUnstable(const std::string& serverIp) {
    LOG(WARNING) << "Server unstable, ip = " << serverIp << "";

    std::vector<ChunkServerID> csIds;
    int ret = mdsclient_->ListChunkServerInServer(serverIp, &csIds);
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "ListChunkServer failed, server ip: " << serverIp;
        return -1;
    }

    for (auto id : csIds) {
        SetChunkserverUnstable(id);
    }

    return 0;
}

void MetaCache::SetChunkserverUnstable(ChunkServerID csid) {
    LOG(WARNING) << "chunkserver " << csid << " unstable!";
    std::set<CopysetIDInfo> copysetIDSet;

    {
        ReadLockGuard rdlk(rwlock4CSCopysetIDMap_);
        auto iter = chunkserverCopysetIDMap_.find(csid);
        if (iter != chunkserverCopysetIDMap_.end()) {
            copysetIDSet = iter->second;
        }
    }

    ReadLockGuard rdlk(rwlock4CopysetInfo_);
    for (auto it : copysetIDSet) {
        const auto key = CalcLogicPoolCopysetID(it.lpid, it.cpid);
        auto cpinfo = lpcsid2CopsetInfoMap_.find(key);
        if (cpinfo != lpcsid2CopsetInfoMap_.end()) {
            ChunkServerID leaderid;
            if (cpinfo->second.GetCurrentLeaderID(&leaderid)) {
                if (leaderid == csid) {
                    //Set only the Lcopyset with leaderdid as the current serverid
                    cpinfo->second.SetLeaderUnstableFlag();
                }
            } else {
                //The current copyset cluster information is unknown, set LeaderUnstable directly
                cpinfo->second.SetLeaderUnstableFlag();
            }
        }
    }
}

void MetaCache::AddCopysetIDInfo(ChunkServerID csid,
                                 const CopysetIDInfo& cpidinfo) {
    WriteLockGuard wrlk(rwlock4CSCopysetIDMap_);
    chunkserverCopysetIDMap_[csid].emplace(cpidinfo);
}

void MetaCache::UpdateChunkserverCopysetInfo(LogicPoolID lpid,
                                 const CopysetInfo<ChunkServerID>& cpinfo) {
    ReadLockGuard rdlk(rwlock4CopysetInfo_);
    const auto key = CalcLogicPoolCopysetID(lpid, cpinfo.cpid_);
    //First, obtain the original chunkserver to copyset mapping
    auto previouscpinfo = lpcsid2CopsetInfoMap_.find(key);
    if (previouscpinfo != lpcsid2CopsetInfoMap_.end()) {
        std::vector<ChunkServerID> newID;
        std::vector<ChunkServerID> changedID;

        //First, determine if the current copyset has changed the chunkserverid
        for (auto iter : previouscpinfo->second.csinfos_) {
            changedID.push_back(iter.peerID);
        }

        for (auto iter : cpinfo.csinfos_) {
            auto it = std::find(changedID.begin(), changedID.end(),
                                iter.peerID);
            if (it != changedID.end()) {
                changedID.erase(it);
            } else {
                newID.push_back(iter.peerID);
            }
        }

        //Delete changed copyset information
        for (auto chunkserverid : changedID) {
            {
                WriteLockGuard wrlk(rwlock4CSCopysetIDMap_);
                auto iter = chunkserverCopysetIDMap_.find(chunkserverid);
                if (iter != chunkserverCopysetIDMap_.end()) {
                    iter->second.erase(CopysetIDInfo(lpid, cpinfo.cpid_));
                }
            }
        }

        //Update new copyset information to chunkserver
        for (auto chunkserverid : newID) {
            WriteLockGuard wrlk(rwlock4CSCopysetIDMap_);
            chunkserverCopysetIDMap_[chunkserverid].emplace(lpid, cpinfo.cpid_);
        }
    }
}

CopysetInfo<ChunkServerID> MetaCache::GetCopysetinfo(
    LogicPoolID lpid, CopysetID csid) {
    ReadLockGuard rdlk(rwlock4CopysetInfo_);
    const auto key = CalcLogicPoolCopysetID(lpid, csid);
    auto cpinfo = lpcsid2CopsetInfoMap_.find(key);
    if (cpinfo != lpcsid2CopsetInfoMap_.end()) {
        return cpinfo->second;
    }
    return CopysetInfo<ChunkServerID>();
}

FileSegment* MetaCache::GetFileSegment(SegmentIndex segmentIndex) {
    {
        ReadLockGuard lk(rwlock4Segments_);
        auto iter = segments_.find(segmentIndex);
        if (iter != segments_.end()) {
            return &iter->second;
        }
    }

    WriteLockGuard lk(rwlock4Segments_);
    auto ret = segments_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(segmentIndex),
        std::forward_as_tuple(segmentIndex,
                              fileInfo_.segmentsize,
                              metacacheopt_.discardGranularity));

    return &(ret.first->second);
}

void MetaCache::CleanChunksInSegment(SegmentIndex segmentIndex) {
    WriteLockGuard lk(rwlock4chunkInfoMap_);
    ChunkIndex beginChunkIndex = static_cast<uint64_t>(segmentIndex) *
                                 fileInfo_.segmentsize / fileInfo_.chunksize;
    ChunkIndex endChunkIndex = static_cast<uint64_t>(segmentIndex + 1) *
                               fileInfo_.segmentsize / fileInfo_.chunksize;

    auto currentIndex = beginChunkIndex;
    while (currentIndex < endChunkIndex) {
        chunkindex2idMap_.erase(currentIndex);
        ++currentIndex;
    }
}

}   // namespace client
}   // namespace curve

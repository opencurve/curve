/*
 * Project: curve
 * File Created: Tuesday, 25th September 2018 2:06:35 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <bthread/bthread.h>

#include <utility>
#include <algorithm>

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

void MetaCache::Init(MetaCacheOption_t metaCacheOpt, MDSClient* mdsclient) {
    mdsclient_ = mdsclient;
    metacacheopt_ = metaCacheOpt;
    confMetric_.getLeaderRetry.set_value(
                metacacheopt_.getLeaderRetry);
    confMetric_.getLeaderTimeOutMs.set_value(
                metacacheopt_.getLeaderTimeOutMs);
    confMetric_.getLeaderRetryIntervalUs.set_value(
                metacacheopt_.retryIntervalUs);
    LOG(INFO) << "metacache init success!"
              << ", get leader retry times = "
              << metacacheopt_.getLeaderRetry
              << ", get leader retry interval us = "
              << metacacheopt_.retryIntervalUs
              << ", get leader rpc time out ms = "
              << metacacheopt_.getLeaderTimeOutMs;
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
                        bool refresh,
                        FileMetric_t* fm) {
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
    if (refresh || targetInfo.LeaderMayChange()) {
        DVLOG(9) << "refresh leader, LogicPoolID = " << logicPoolId
                  << ", CopysetID = " << copysetId;
        uint32_t retry = 0;
        while (retry++ < metacacheopt_.getLeaderRetry) {
            ret = UpdateLeaderInternal(logicPoolId, copysetId, &targetInfo, fm);
            if (ret != -1) {
                targetInfo.ResetSetLeaderUnstableFlag();
                UpdateCopysetInfo(logicPoolId, copysetId, targetInfo);
                break;
            } else {
                LOG(INFO) << "refresh leader from chunkserver failed, "
                          << "get copyset chunkserver list from mds, "
                          << "copyset id = " << copysetId << ", "
                          << "logicpool id = " << logicPoolId;
                // 重试失败，这时候需要向mds重新拉取最新的copyset信息了
                // JIRA: http://jira.netease.com/browse/CLDCFS-1262
                std::vector<CopysetID> copysetidvec;
                copysetidvec.push_back(copysetId);
                std::vector<CopysetInfo_t> cpinfoVec;
                ret = mdsclient_->GetServerList(logicPoolId,
                                                copysetidvec,
                                                &cpinfoVec);
                if (ret == LIBCURVE_ERROR::OK && !cpinfoVec.empty()) {
                    // 更新chunkserverid到copyset映射关系
                    UpdateChunkserverCopysetInfo(logicPoolId, cpinfoVec[0]);
                    // 更新logicpool和copysetid到copysetinfo的映射
                    UpdateCopysetInfo(logicPoolId, copysetId, cpinfoVec[0]);
                    break;
                } else {
                    LOG(ERROR) << "get copyset server list info from mds failed"
                               << ", copyset id = " << copysetId
                               << ", logicpool id = " << logicPoolId;
                }
            }
            bthread_usleep(metacacheopt_.retryIntervalUs);
        }
    }

    if (ret == -1) {
        LOG(ERROR) << "get leader failed after retry!"
                   << ", copyset id = " << copysetId
                   << ", logicpool id = " << logicPoolId;
        return -1;
    }

    return targetInfo.GetLeaderInfo(serverId, serverAddr);
}

int MetaCache::UpdateLeaderInternal(LogicPoolID logicPoolId,
                                    CopysetID copysetId,
                                    CopysetInfo* toupdateCopyset,
                                    FileMetric_t* fm) {
    ChunkServerID csid = 0;
    ChunkServerAddr  leaderaddr;
    int ret = ServiceHelper::GetLeader(logicPoolId, copysetId,
                                      toupdateCopyset->csinfos_, &leaderaddr,
                                      toupdateCopyset->GetCurrentLeaderIndex(),
                                      metacacheopt_.getLeaderTimeOutMs,
                                      &csid,
                                      fm);

    if (ret == -1) {
        LOG(ERROR) << "get leader failed!"
                   << ", copyset id = " << copysetId
                   << ", logicpool id = " << logicPoolId;
        return -1;
    }
    return toupdateCopyset->UpdateLeaderInfo(csid, leaderaddr);
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

bool MetaCache::CopysetIDInfoIn(ChunkServerID csid,
                                LogicPoolID lpid,
                                CopysetID cpid) {
    std::set<CopysetIDInfo> copysetIDSet;
    ReadLockGuard rdlk(rwlock4CSCopysetIDMap_);
    auto iter = chunkserverCopysetIDMap_.find(csid);
    if (iter != chunkserverCopysetIDMap_.end()) {
        copysetIDSet = iter->second;
        auto it = copysetIDSet.find(CopysetIDInfo(lpid, cpid));
        return it != copysetIDSet.end();
    }
    return false;
}

void MetaCache::SetChunkserverUnstable(ChunkServerID csid) {
    {
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
            std::string mapkey = LogicPoolCopysetID2Str(it.lpid, it.cpid);
            auto cpinfo = lpcsid2CopsetInfoMap_.find(mapkey);
            if (cpinfo != lpcsid2CopsetInfoMap_.end()) {
                ChunkServerID leaderid;
                if (cpinfo->second.GetCurrentLeaderServerID(&leaderid)) {
                    if (leaderid == csid) {
                        // 只设置leaderid为当前serverid的Lcopyset
                        cpinfo->second.SetLeaderUnstableFlag();
                    }
                } else {
                    // 当前copyset集群信息未知，直接设置LeaderUnStable
                    cpinfo->second.SetLeaderUnstableFlag();
                }
            }
        }
    }
}

void MetaCache::AddCopysetIDInfo(ChunkServerID csid,
                                const CopysetIDInfo& cpidinfo) {
    {
        WriteLockGuard wrlk(rwlock4CSCopysetIDMap_);
        auto iter = chunkserverCopysetIDMap_.find(csid);
        if (iter != chunkserverCopysetIDMap_.end()) {
            if (iter->second.find(cpidinfo) == iter->second.end()) {
                iter->second.insert(cpidinfo);
            }
        } else {
            std::set<CopysetIDInfo> cpidinfoSet;
            cpidinfoSet.insert(cpidinfo);
            chunkserverCopysetIDMap_[csid] = cpidinfoSet;
        }
    }
}

void MetaCache::UpdateChunkserverCopysetInfo(LogicPoolID lpid,
                                            const CopysetInfo_t& cpinfo) {
    ReadLockGuard rdlk(rwlock4CopysetInfo_);
    std::string mapkey = LogicPoolCopysetID2Str(lpid, cpinfo.cpid_);
    // 先获取原来的chunkserver到copyset映射
    auto previouscpinfo = lpcsid2CopsetInfoMap_.find(mapkey);
    if (previouscpinfo != lpcsid2CopsetInfoMap_.end()) {
        std::vector<ChunkServerID> newID;
        std::vector<ChunkServerID> changedID;

        // 先判断当前copyset有没有变更chunkserverid
        for (auto iter : previouscpinfo->second.csinfos_) {
            changedID.push_back(iter.chunkserverid_);
        }

        for (auto iter : cpinfo.csinfos_) {
            auto it = std::find(changedID.begin(), changedID.end(),
                                iter.chunkserverid_);
            if (it != changedID.end()) {
                changedID.erase(it);
            } else {
                newID.push_back(iter.chunkserverid_);
            }
        }

        // 删除变更的copyset信息
        for (auto chunkserverid : changedID) {
            {
                WriteLockGuard wrlk(rwlock4CSCopysetIDMap_);
                auto iter = chunkserverCopysetIDMap_.find(chunkserverid);
                if (iter != chunkserverCopysetIDMap_.end()) {
                    iter->second.erase(CopysetIDInfo(lpid, cpinfo.cpid_));
                }
            }
        }

        // 更新新的copyset信息到chunkserver
        for (auto chunkserverid : newID) {
            {
                WriteLockGuard wrlk(rwlock4CSCopysetIDMap_);
                auto iter = chunkserverCopysetIDMap_.find(chunkserverid);
                if (iter == chunkserverCopysetIDMap_.end()) {
                    std::set<CopysetIDInfo> cpidinfoSet;
                    cpidinfoSet.insert(CopysetIDInfo(lpid, cpinfo.cpid_));
                    chunkserverCopysetIDMap_[chunkserverid] = cpidinfoSet;
                } else {
                    iter->second.insert(CopysetIDInfo(lpid, cpinfo.cpid_));
                }
            }
        }
    }
}

CopysetInfo_t MetaCache::GetCopysetinfo(LogicPoolID lpid, CopysetID csid) {
    ReadLockGuard rdlk(rwlock4CopysetInfo_);
    std::string mapkey = LogicPoolCopysetID2Str(lpid, csid);
    auto cpinfo = lpcsid2CopsetInfoMap_.find(mapkey);
    if (cpinfo != lpcsid2CopsetInfoMap_.end()) {
        return cpinfo->second;
    }
    return CopysetInfo();
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

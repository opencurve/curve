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
 * Created Date: Thur Sept 4 2021
 * Author: lixiaocui
 */

#include <butil/fast_rand.h>
#include <algorithm>
#include <iterator>
#include <vector>
#include <map>
#include <utility>
#include "curvefs/src/client/rpcclient/metacache.h"
#include "src/client/metacache.h"

using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

namespace curvefs {
namespace client {
namespace rpcclient {

void MetaCache::SetTxId(uint32_t partitionId, uint64_t txId) {
    WriteLockGuard w(txIdLock_);
    partitionTxId_[partitionId] = txId;
}

// partitionTxId_: cache for partition's txid
void MetaCache::GetTxId(uint32_t partitionId, uint64_t *txId) {
    ReadLockGuard r(txIdLock_);
    auto iter = partitionTxId_.find(partitionId);
    if (iter != partitionTxId_.end()) {
        *txId = iter->second;
    }
}

bool MetaCache::GetTxId(uint32_t fsId,
                        uint64_t inodeId,
                        uint32_t *partitionId,
                        uint64_t *txId) {
    for (const auto &partition : partitionInfos_) {
        if (fsId == partition.fsid() &&
            inodeId >= partition.start() && inodeId <= partition.end()) {
            *partitionId = partition.partitionid();
            *txId = partition.txid();
            GetTxId(*partitionId, txId);
            return true;
        }
    }
    return false;
}

void MetaCache::GetAllTxIds(std::vector<PartitionTxId> *txIds) {
    ReadLockGuard r(txIdLock_);
    auto iter = partitionTxId_.begin();
    for (; iter != partitionTxId_.end(); iter++) {
        PartitionTxId tmp;
        tmp.set_partitionid(iter->first);
        tmp.set_txid(iter->second);
        txIds->push_back(std::move(tmp));
    }
}

bool MetaCache::RefreshTxId() {
    std::vector<PartitionTxId> txIds;
    FSStatusCode rc = mdsClient_->GetLatestTxId(&txIds);
    if (rc != FSStatusCode::OK) {
        LOG(ERROR) << "Get latest txid failed, retCode=" << rc;
        return false;
    }

    for (const auto& item : txIds) {
        SetTxId(item.partitionid(), item.txid());
    }
    return true;
}

bool MetaCache::GetTarget(uint32_t fsID, uint64_t inodeID,
                          CopysetTarget *target, uint64_t *applyIndex,
                          bool refresh) {
    // get copysetID with inodeID
    if (!GetCopysetIDwithInodeID(inodeID, &target->groupID,
                                 &target->partitionID, &target->txId)) {
        // list infos from mds
        if (!ListPartitions(fsID)) {
            LOG(ERROR) << "get target for {fsid:" << fsID
                       << "} fail, partition list not exist";
            return false;
        }

        if (!GetCopysetIDwithInodeID(inodeID, &target->groupID,
                                     &target->partitionID, &target->txId)) {
            LOG(ERROR) << "{fsid:" << fsID << ", inodeid:" << inodeID
                       << "} do not find partition";
            return false;
        }
    }

    // get target copyset leader with (poolID, copysetID)
    return GetTargetLeader(target, applyIndex, refresh);
}

bool MetaCache::SelectTarget(uint32_t fsID, CopysetTarget *target,
                             uint64_t *applyIndex) {
    // select a partition
    if (!SelectPartition(target)) {
        // list from mds
        if (!ListPartitions(fsID)) {
            LOG(ERROR) << "select target for {fsid:" << fsID
                       << "} fail,  list info from mds fail";
            return false;
        }

        if (!SelectPartition(target)) {
            LOG(ERROR) << "select target for {fsid:" << fsID
                       << "} fail,  select paritiotn fail";
            return false;
        }
    }

    // get target copyset leader with (poolID, copysetID)
    return GetTargetLeader(target, applyIndex);
}

void MetaCache::UpdateApplyIndex(const CopysetGroupID &groupID,
                                 uint64_t applyIndex) {
    const auto key = CalcLogicPoolCopysetID(groupID);

    ReadLockGuard rl(rwlock4copysetInfoMap_);
    auto iter = copysetInfoMap_.find(key);
    if (iter == copysetInfoMap_.end()) {
        LOG(WARNING) << "update apply index for copyset:" << groupID.ToString()
                     << " fail, copyset not found.";
        return;
    }

    iter->second.UpdateAppliedIndex(applyIndex);
}

uint64_t MetaCache::GetApplyIndex(const CopysetGroupID &groupID) {
    const auto key = CalcLogicPoolCopysetID(groupID);

    ReadLockGuard rl(rwlock4copysetInfoMap_);
    auto iter = copysetInfoMap_.find(key);
    if (iter == copysetInfoMap_.end()) {
        LOG(WARNING) << "get apply index for copyset:" << groupID.ToString()
                     << " fail, copyset not found.";
        return 0;
    }

    return iter->second.GetAppliedIndex();
}

bool MetaCache::IsLeaderMayChange(const CopysetGroupID &groupID) {
    const auto key = CalcLogicPoolCopysetID(groupID);


    ReadLockGuard rl(rwlock4copysetInfoMap_);
    auto iter = copysetInfoMap_.find(key);
    if (iter == copysetInfoMap_.end()) {
        LOG(WARNING) << "get apply index for copyset:" << groupID.ToString()
                     << " fail, copyset not found.";
        return false;
    }

    return iter->second.LeaderMayChange();
}

void MetaCache::UpdateCopysetInfo(const CopysetGroupID &groupID,
                                  const CopysetInfo<MetaserverID> &csinfo) {
    const auto key = CalcLogicPoolCopysetID(groupID);

    WriteLockGuard wl(rwlock4copysetInfoMap_);
    copysetInfoMap_[key] = csinfo;
}

bool MetaCache::GetTargetLeader(CopysetTarget *target, uint64_t *applyindex,
                                bool refresh) {
    // get copyset with (poolid, copysetid)
    CopysetInfo<MetaserverID> copysetInfo;
    if (!GetCopysetInfowithCopySetID(target->groupID, &copysetInfo)) {
        LOG(WARNING) << ", copyset:" << target->groupID.ToString()
                     << "} do not find copyset info";
        return false;
    }

    // get copyset leader from metacache
    if (!refresh && !copysetInfo.LeaderMayChange()) {
        if (0 == copysetInfo.GetLeaderInfo(&target->metaServerID,
                                           &target->endPoint)) {
            return true;
        }
        LOG(WARNING) << "{copyset:" << target->groupID.ToString()
                     << "} get leader from cache fail."
                     << " current leader:" << target->metaServerID;
    }

    // if cacahe do not have invalid leader, refresh leader
    VLOG(3) << "refresh leader for " << target->groupID.ToString();
    bool ret = true;
    uint32_t retry = 0;
    while (retry++ < metacacheopt_.metacacheGetLeaderRetry) {
        // refresh from metaserver
        ret = UpdateLeaderInternal(target->groupID, &copysetInfo);
        if (ret) {
            copysetInfo.ResetSetLeaderUnstableFlag();
            UpdateCopysetInfo(target->groupID, copysetInfo);
            break;
        } else {
            VLOG(3) << "refresh leader from metaserver failed, "
                    << "get copyset peer list from mds, copyset:"
                    << target->groupID.ToString();
        }

        // refresh from mds
        ret = UpdateCopysetInfoFromMDS(target->groupID, &copysetInfo);
        if (ret) {
            break;
        }

        bthread_usleep(metacacheopt_.metacacheRPCRetryIntervalUS);
    }

    if (!ret) {
        LOG(WARNING) << "get leader failed after retry!"
                     << ", copyset:" << target->groupID.ToString();
        return false;
    }

    *applyindex = copysetInfo.GetAppliedIndex();
    return 0 ==
           copysetInfo.GetLeaderInfo(&target->metaServerID, &target->endPoint);
}

bool MetaCache::ListPartitions(uint32_t fsID) {
    WriteLockGuard wl4PartitionMap(rwlock4Partitions_);
    WriteLockGuard wl4CopysetMap(rwlock4copysetInfoMap_);

    fsID_ = fsID;
    PatitionInfoList partitionInfos;
    std::map<PoolIDCopysetID, CopysetInfo<MetaserverID>> copysetMap;
    if (!DoListOrCreatePartitions(true, &partitionInfos, &copysetMap)) {
        return false;
    }

    DoAddOrResetPartitionAndCopyset(std::move(partitionInfos),
                                    std::move(copysetMap), true);
    return true;
}

bool MetaCache::CreatePartitions(int currentNum,
                                 PatitionInfoList *newPartitions) {
    std::lock_guard<Mutex> lg(createMutex_);

    // already create
    {
        ReadLockGuard rl(rwlock4Partitions_);
        if (partitionInfos_.size() > currentNum) {
            return true;
        }
    }

    // create partition
    std::map<PoolIDCopysetID, CopysetInfo<MetaserverID>> copysetMap;
    if (!DoListOrCreatePartitions(false, newPartitions, &copysetMap)) {
        return false;
    }

    // add partition and copyset info
    WriteLockGuard wl4PartitionMap(rwlock4Partitions_);
    WriteLockGuard wl4CopysetMap(rwlock4copysetInfoMap_);
    DoAddOrResetPartitionAndCopyset(*newPartitions,
                                    std::move(copysetMap), false);

    return true;
}

bool MetaCache::DoListOrCreatePartitions(
    bool list, PatitionInfoList *partitionInfos,
    std::map<PoolIDCopysetID, CopysetInfo<MetaserverID>> *copysetMap) {
    // TODO(@lixiaocui): list or get partition need too many rpc,
    // it's better to return all infos once.
    if (list) {
        if (!mdsClient_->ListPartition(fsID_, partitionInfos)) {
            LOG(ERROR) << "list parition for {fsid:" << fsID_ << "} fail";
            return false;
        }
    } else {
        if (!mdsClient_->CreatePartition(
                fsID_, metacacheopt_.createPartitionOnce, partitionInfos)) {
            LOG(ERROR) << "create partition for fsid:" << fsID_ << " fail";
            return false;
        }
    }

    // gather partitionIDs
    std::vector<PartitionID> partitionIDList;
    for_each(partitionInfos->begin(), partitionInfos->end(),
             [&](const PartitionInfo &info) {
                 auto key = CalcLogicPoolCopysetID(
                     CopysetGroupID(info.poolid(), info.copysetid()));
                 bool exist = false;

                 if (!list) {
                     ReadLockGuard rl(rwlock4copysetInfoMap_);
                     exist = (copysetInfoMap_.count(key) > 0);
                 }

                 if (!exist) {
                     partitionIDList.emplace_back(info.partitionid());
                 }
             });

    if (partitionIDList.empty()) {
        return true;
    }

    // get copyset for each partition
    std::map<PartitionID, Copyset> copysets;
    if (!mdsClient_->GetCopysetOfPartitions(partitionIDList, &copysets)) {
        LOG(ERROR) << "get copyset infos of partitions for fsid:" << fsID_
                   << " fail";
        return false;
    }

    std::map<LogicPoolID, std::vector<CopysetID>> lpool2Copyset;
    for_each(copysets.begin(), copysets.end(),
             [&](const std::pair<PartitionID, Copyset> &item) {
                 lpool2Copyset[item.second.poolid()].emplace_back(
                     item.second.copysetid());
             });

    // get copyset infos
    bool ok = false;
    for (auto iter = lpool2Copyset.begin(); iter != lpool2Copyset.end();
         ++iter) {
        // get copyset info
        std::vector<CopysetInfo<MetaserverID>> cpinfoVec;
        if (!mdsClient_->GetMetaServerListInCopysets(iter->first, iter->second,
                                                     &cpinfoVec)) {
            LOG(ERROR) << "get metaserver list of copyset in {poolid:"
                       << iter->first << "} fail";
            ok = false;
            break;
        }
        for_each(cpinfoVec.begin(), cpinfoVec.end(),
                 [&](const CopysetInfo<MetaserverID> &info) {
                     const auto key = CalcLogicPoolCopysetID(
                         CopysetGroupID(iter->first, info.cpid_));
                     (*copysetMap)[key] = info;
                 });

        ok = true;
    }

    return ok;
}

void MetaCache::DoAddOrResetPartitionAndCopyset(
    PatitionInfoList partitionInfos,
    std::map<PoolIDCopysetID, CopysetInfo<MetaserverID>> copysetMap,
    bool reset) {
    if (reset) {
        partitionInfos_.clear();
        copysetInfoMap_.clear();
    }

    LOG(INFO) << "add partition and copyset infos for {fsid:" << fsID_
              << "} ok, partition size = " << partitionInfos.size()
              << ", copyset size = " << copysetMap.size()
              << ", reset = " << reset;
    // add partitionIxid
    std::for_each(partitionInfos.begin(), partitionInfos.end(),
                  [&](const PartitionInfo &item) {
                      SetTxId(item.partitionid(), item.txid());
                  });
    // add partitionInfo
    partitionInfos_.insert(partitionInfos_.end(),
                           std::make_move_iterator(partitionInfos.begin()),
                           std::make_move_iterator(partitionInfos.end()));
    // add copysetInfo
    copysetInfoMap_.insert(std::make_move_iterator(copysetMap.begin()),
                           std::make_move_iterator(copysetMap.end()));
}

bool MetaCache::UpdateCopysetInfoFromMDS(
    const CopysetGroupID &groupID, CopysetInfo<MetaserverID> *targetInfo) {
    std::vector<CopysetInfo<MetaserverID>> metaServerInfos;

    bool ret = mdsClient_->GetMetaServerListInCopysets(
        groupID.poolID, {groupID.copysetID}, &metaServerInfos);

    if (!ret || metaServerInfos.empty()) {
        LOG(WARNING) << "Get copyset server list from mds return empty server "
                        "list, ret = "
                     << ret << ", copyset:" << groupID.ToString();
        return false;
    }

    UpdateCopysetInfo(groupID, metaServerInfos[0]);
    *targetInfo = metaServerInfos[0];

    return true;
}

bool MetaCache::UpdateLeaderInternal(
    const CopysetGroupID &groupID, CopysetInfo<MetaserverID> *toupdateCopyset) {
    MetaserverID metaserverID = 0;
    PeerAddr leaderAddr;

    // get leader from peers
    bool getLeaderOk = cli2Client_->GetLeader(
        groupID.poolID, groupID.copysetID, toupdateCopyset->csinfos_,
        toupdateCopyset->leaderindex_, &leaderAddr, &metaserverID);
    if (!getLeaderOk) {
        LOG(WARNING) << "get leader failed!"
                     << ", copyset:" << groupID.ToString();
        return false;
    }

    // update leader info, if fail, update copyset info from mds
    int ret = toupdateCopyset->UpdateLeaderInfo(leaderAddr);
    if (ret == -1 && !leaderAddr.IsEmpty()) {
        CopysetPeerInfo<MetaserverID> metaserverInfo;
        bool ret = mdsClient_->GetMetaServerInfo(leaderAddr, &metaserverInfo);
        if (false == ret) {
            LOG(ERROR) << "get metaserver id from mds failed, addr = "
                       << leaderAddr.ToString();
            return false;
        }

        UpdateCopysetInfoIfMatchCurrentLeader(groupID, leaderAddr);
        GetCopysetInfowithCopySetID(groupID, toupdateCopyset);
        ret = toupdateCopyset->UpdateLeaderInfo(leaderAddr, metaserverInfo);
    }

    return true;
}

bool MetaCache::MarkPartitionUnavailable(PartitionID pid) {
    WriteLockGuard wl(rwlock4Partitions_);

    for (auto iter = partitionInfos_.begin(); iter != partitionInfos_.end();
         iter++) {
        if (iter->partitionid() == pid) {
            iter->set_status(PartitionStatus::READONLY);
            break;
        }
    }
    return true;
}

void MetaCache::UpdateCopysetInfoIfMatchCurrentLeader(
    const CopysetGroupID &groupID, const PeerAddr &leaderAddr) {
    std::vector<CopysetInfo<MetaserverID>> metaServerInfos;
    bool ret = mdsClient_->GetMetaServerListInCopysets(
        groupID.poolID, {groupID.copysetID}, &metaServerInfos);

    bool needUpdate = (!metaServerInfos.empty()) &&
                      (metaServerInfos[0].HasPeerInCopyset(leaderAddr));
    if (needUpdate) {
        LOG(INFO) << "Update copyset info " << groupID.ToString()
                  << ", current leader = " << leaderAddr.ToString();
        UpdateCopysetInfo(groupID, metaServerInfos[0]);
    }
}

bool MetaCache::SelectPartition(CopysetTarget *target) {
    // exclude partition which is readonly
    std::map<PartitionID, PartitionInfo> candidate;
    int currentNum = 0;
    {
        ReadLockGuard rl(rwlock4Partitions_);
        currentNum = partitionInfos_.size();
        for_each(partitionInfos_.begin(), partitionInfos_.end(),
                 [&](const PartitionInfo &pInfo) {
                     if (pInfo.status() == PartitionStatus::READWRITE) {
                         candidate[pInfo.partitionid()] = pInfo;
                     }
                 });
    }

    if (candidate.empty()) {
        // create parition for fs
        LOG(INFO) << "no partition can be select for fsid:" << fsID_
                  << ", need create new partitions";
        PatitionInfoList newPartitions;
        if (!CreatePartitions(currentNum, &newPartitions)) {
            LOG(ERROR) << "create partition for fsid:" << fsID_ << " fail";
            return false;
        }
        target->groupID = CopysetGroupID(newPartitions[0].poolid(),
                                         newPartitions[0].copysetid());
        target->partitionID = newPartitions[0].partitionid();
        target->txId = newPartitions[0].txid();
    } else {
        // random select a partition
        const auto index = butil::fast_rand() % candidate.size();
        auto iter = candidate.begin();
        std::advance(iter, index);
        target->groupID =
            CopysetGroupID(iter->second.poolid(), iter->second.copysetid());
        target->partitionID = iter->first;
        target->txId = iter->second.txid();
    }

    return true;
}

bool MetaCache::GetCopysetIDwithInodeID(uint64_t inodeID,
                                        CopysetGroupID *groupID,
                                        PartitionID *partitionID,
                                        uint64_t *txId) {
    ReadLockGuard rl(rwlock4Partitions_);
    for (auto iter = partitionInfos_.begin(); iter != partitionInfos_.end();
         ++iter) {
        if (iter->start() <= inodeID && iter->end() >= inodeID) {
            *groupID = CopysetGroupID(iter->poolid(), iter->copysetid());
            *partitionID = iter->partitionid();
            *txId = iter->txid();
            GetTxId(*partitionID, txId);
            return true;
        }
    }
    return false;
}

bool MetaCache::GetCopysetInfowithCopySetID(
    const CopysetGroupID &groupID, CopysetInfo<MetaserverID> *targetInfo) {
    const auto key = CalcLogicPoolCopysetID(groupID);
    ReadLockGuard rl(rwlock4copysetInfoMap_);
    auto iter = copysetInfoMap_.find(key);
    if (iter == copysetInfoMap_.end()) {
        return false;
    }

    *targetInfo = iter->second;
    return true;
}

bool TryGetPartitionIdByInodeId(const std::vector<PartitionInfo> &plist,
    RWLock *lock, uint64_t inodeID, PartitionID *pid) {
    ReadLockGuard rl(*lock);
    for (const auto &it : plist) {
        if (it.start() <= inodeID && it.end() >= inodeID) {
            *pid = it.partitionid();
            return true;
        }
    }
    return false;
}

bool MetaCache::GetPartitionIdByInodeId(uint32_t fsID, uint64_t inodeID,
    PartitionID *pid) {
    if (!TryGetPartitionIdByInodeId(partitionInfos_,
                                    &rwlock4Partitions_, inodeID, pid)) {
        // list form mds
        if (!ListPartitions(fsID)) {
            LOG(ERROR) << "ListPartitions for {fsid:" << fsID
                       << "} fail, partition list not exist";
            return false;
        }
        return TryGetPartitionIdByInodeId(partitionInfos_,
            &rwlock4Partitions_, inodeID, pid);
    }
    return true;
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

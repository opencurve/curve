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

#include <iterator>
#include <vector>
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

void MetaCache::GetTxId(uint32_t partitionId, uint64_t* txId) {
    ReadLockGuard r(txIdLock_);
    auto iter = partitionTxId_.find(partitionId);
    if (iter != partitionTxId_.end()) {
        *txId = iter->second;
    }
}

bool MetaCache::GetTxId(uint32_t fsId,
                        uint64_t inodeId,
                        uint32_t* partitionId,
                        uint64_t* txId) {
    PatitionInfoList partitions;
    if (GetParitionListWihFsID(fsId, &partitions)) {
        for (const auto& partition : partitions) {
            if (inodeId >= partition.start() && inodeId <= partition.end()) {
                *partitionId = partition.partitionid();
                *txId = partition.txid();
                GetTxId(*partitionId, txId);
                return true;
            }
        }
    }

    return false;
}

bool MetaCache::GetTarget(uint32_t fsID, uint64_t inodeID,
                          CopysetTarget *target, uint64_t *applyIndex,
                          bool refresh) {
    // get partitionlist with fsid
    PatitionInfoList pinfoList;
    if (false == GetParitionListWihFsID(fsID, &pinfoList)) {
        LOG(WARNING) << "{fsid:" << fsID << "} parition list no exist";
        return false;
    }

    // get copysetID with inodeID
    if (false == GetCopysetIDwithInodeID(pinfoList, inodeID, &target->groupID,
                                         &target->partitionID, &target->txId)) {
        LOG(WARNING) << "{fsid:" << fsID << ", inodeid:" << inodeID
                     << "} do not find partition";
        return false;
    }

    // get copysetInfo with (poolID, copysetID)
    CopysetInfo<MetaserverID> copysetInfo;
    if (false == GetCopysetInfowithCopySetID(target->groupID, &copysetInfo)) {
        LOG(WARNING) << "{fsid:" << fsID << ", inodeid:" << inodeID
                     << ", copyset:" << target->groupID.ToString()
                     << "} do not find copyset info";
        return false;
    }

    if (false == refresh && false == copysetInfo.LeaderMayChange()) {
        if (0 == copysetInfo.GetLeaderInfo(&target->metaServerID,
                                           &target->endPoint)) {
            return true;
        }

        LOG(WARNING) << "{fsid:" << fsID << ", inodeid:" << inodeID
                     << ", copyset:" << target->groupID.ToString()
                     << "} get leader from cache fail."
                     << " current leader:" << target->metaServerID;
    }

    // when need refresh or leader change, we need to update copysetinfo
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
            LOG(INFO) << "refresh leader from metaserver failed, "
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

    *applyIndex = copysetInfo.GetAppliedIndex();
    return 0 ==
           copysetInfo.GetLeaderInfo(&target->metaServerID, &target->endPoint);
}

bool MetaCache::SelectTarget(uint32_t fsID, CopysetTarget *target,
                             uint64_t *applyIndex) {
    // get partitionlist with fsid
    PatitionInfoList pinfoList;
    if (false == GetParitionListWihFsID(fsID, &pinfoList)) {
        LOG(WARNING) << "select target for {fsid:" << fsID
                     << "} fail, parition list not exist";
        return false;
    }

    // random select a partition from list
    int index = rand() % pinfoList.size();  // NOLINT
    auto iter = pinfoList.begin();
    std::advance(iter, index);
    target->groupID =
        std::move(CopysetGroupID(iter->poolid(), iter->copysetid()));
    target->partitionID = iter->partitionid();

    // get copysetInfo with coysetID
    CopysetInfo<MetaserverID> copysetInfo;
    if (!GetCopysetInfowithCopySetID(target->groupID, &copysetInfo)) {
        LOG(WARNING) << "select target for {fsid:" << fsID
                     << ", copyset:" << target->groupID.ToString()
                     << "} fail, do not find copyset info";
        return false;
    }

    *applyIndex = copysetInfo.GetAppliedIndex();
    return 0 ==
           copysetInfo.GetLeaderInfo(&target->metaServerID, &target->endPoint);
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

void MetaCache::UpdateCopysetInfo(const CopysetGroupID &groupID,
                                  const CopysetInfo<MetaserverID> &csinfo) {
    const auto key = CalcLogicPoolCopysetID(groupID);

    WriteLockGuard wl(rwlock4copysetInfoMap_);
    copysetInfoMap_[key] = csinfo;
}

void MetaCache::UpdatePartitionInfo(uint32_t fsID,
                                    const PatitionInfoList &pInfoList) {
    WriteLockGuard wl(rwlock4fs2PartitionInfoMap_);
    fs2PartitionInfoMap_[fsID] = pInfoList;
}


bool MetaCache::GetParitionListWihFsID(uint32_t fsID,
                                       PatitionInfoList *pinfoList) {
    ReadLockGuard rl(rwlock4fs2PartitionInfoMap_);
    auto iter = fs2PartitionInfoMap_.find(fsID);
    if (iter == fs2PartitionInfoMap_.end()) {
        return false;
    }
    *pinfoList = iter->second;
    if (pinfoList->empty()) {
        return false;
    }
    return true;
}

bool MetaCache::GetCopysetIDwithInodeID(const PatitionInfoList &pinfoList,
                                        uint32_t inodeID,
                                        CopysetGroupID *groupID,
                                        PartitionID *partitionID,
                                        uint64_t* txId) {
    bool find = false;
    for_each(
        pinfoList.begin(), pinfoList.end(), [&](const PartitionInfo &pinfo) {
            if (!find && pinfo.start() <= inodeID && pinfo.end() >= inodeID) {
                find = true;
                *groupID = std::move(
                    CopysetGroupID(pinfo.poolid(), pinfo.copysetid()));
                *partitionID = pinfo.partitionid();
                *txId = pinfo.txid();  // original txid
                GetTxId(*partitionID, txId);
            }
        });

    return find;
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

bool MetaCache::UpdateCopysetInfoFromMDS(
    const CopysetGroupID &groupID, CopysetInfo<MetaserverID> *targetInfo) {
    std::vector<CopysetInfo<MetaserverID>> metaServerInfos;

    bool ret = mdsClient_->GetMetaServerListInCopysets(
        groupID.poolID, {groupID.copysetID}, &metaServerInfos);

    if (metaServerInfos.empty()) {
        LOG(WARNING) << "Get copyset server list from mds return empty server "
                        "list, ret = "
                     << ret << ", copyset:" << groupID.ToString();
        return false;
    }

    UpdateCopysetInfo(groupID, metaServerInfos[0]);
    *targetInfo = metaServerInfos[0];

    return true;
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

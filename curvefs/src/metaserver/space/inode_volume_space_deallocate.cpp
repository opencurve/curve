/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Date: Wed Mar 22 10:38:14 CST 2023
 * Author: lixiaocui
 */

#include <glog/logging.h>
#include <algorithm>
#include <iterator>
#include <unordered_map>
#include "curvefs/src/metaserver/space/inode_volume_space_deallocate.h"
#include "curvefs/src/client/async_request_closure.h"

using curvefs::client::UpdateVolumeExtentClosure;

namespace curvefs {
namespace metaserver {

void InodeVolumeSpaceDeallocate::CalDeallocatableSpace() {
    // get all deallocatable inode
    auto iter = calOpt_.kvStorage->HGetAll(
        calOpt_.nameGen->GetDeallocatableInodeTableName());
    if (iter->Status() != 0) {
        LOG(ERROR) << "InodeVolumeSpaceDeallocate failed to get iterator for "
                      "all deallocatable indoe";
        return;
    } else {
        iter->SeekToFirst();
    }

    // key is volume offset
    DeallocatableBlockGroupMap increase;
    while (iter->Valid()) {
        Key4Inode key;
        bool ok = calOpt_.conv.ParseFromString(iter->Key(), &key);
        uint64_t typicalInode = key.inodeId;
        iter->Next();
        if (!ok) {
            LOG(ERROR) << "InodeVolumeSpaceDeallocate parse inode key from "
                       << iter->Key() << "fail";
            continue;
        }

        // fill IncreaseDeallocatableBlockGroup
        ok = DeallocatableSapceForInode(key, &increase);
        if (!ok) {
            continue;
        }

        if (increase.size() >= executeOpt_.batchClean || !iter->Valid()) {
            // update according to metaserver client
            MetaStatusCode st =
                executeOpt_.metaClient->UpdateDeallocatableBlockGroup(
                    fsId_, typicalInode, &increase);
            if (st != MetaStatusCode::OK) {
                LOG(ERROR) << "InodeVolumeSpaceDeallocate update "
                           << "deallocatable block group fail";
            }
            LOG(INFO) << "InodeVolumeSpaceDeallocate cal success this round";
            increase.clear();
        }
    }
}

MetaStatusCode
InodeVolumeSpaceDeallocate::DeallocateOneBlockGroup(uint64_t blockGroupOffset) {
    // get block group
    auto skey = calOpt_.conv.SerializeToString(
        Key4DeallocatableBlockGroup{fsId_, blockGroupOffset});

    DeallocatableBlockGroup out;
    auto s = calOpt_.kvStorage->HGet(
        calOpt_.nameGen->GetDeallocatableBlockGroupTableName(), skey, &out);
    if (s.IsNotFound()) {
        LOG(INFO) << "InodeVolumeSpaceDeallocate do not record deallocatable "
                     "blockgroup, "
                     "fsId="
                  << fsId_ << ", blockGroupOffset=" << blockGroupOffset;
        return MetaStatusCode::OK;
    }
    if (!s.ok()) {
        LOG(ERROR) << "InodeVolumeSpaceDeallocate deallocate blockgroup fail, "
                      "fsId="
                   << fsId_ << ", blockGroupOffset=" << blockGroupOffset
                   << ", partitionId=" << partitionId_
                   << ", status=" << s.ToString();
        return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }

    // batch processing of inodelists involving space deallocatable
    DeallocatableBlockGroupMap mark;
    auto &onemark = mark[blockGroupOffset];
    onemark.set_blockgroupoffset(blockGroupOffset);

    auto iter = out.inodeidlist().begin();
    auto boundary = out.inodeidlist().end();
    while (iter != boundary) {
        onemark.mutable_mark()->add_inodeidunderdeallocate(*iter);
        iter++;

        auto size = onemark.mark().inodeidunderdeallocate_size();
        if (size >= executeOpt_.batchClean || (iter == boundary && size > 0)) {
            ProcessSepcifyInodeList(blockGroupOffset, &mark);
            onemark.mutable_mark()->clear_inodeidunderdeallocate();
        }
    }

    return MetaStatusCode::OK;
}

bool InodeVolumeSpaceDeallocate::DeallocatableSapceForInode(
    const Key4Inode &key, DeallocatableBlockGroupMap *increaseMap) {

    // Calculate the deallocatable space of the blockgroup corresponding to the
    // extent in the inode
    auto iter = calOpt_.inodeStorage->GetAllVolumeExtent(key.fsId, key.inodeId);

    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        VolumeExtentSlice slice;
        if (!slice.ParseFromString(iter->Value())) {
            LOG(ERROR) << "InodeVolumeSpaceDeallocate parse VolumeExtentSlice "
                          "failed, fsId="
                       << key.fsId << ", inodeId=" << key.inodeId;
            return false;
        }

        DeallocatbleSpaceForVolumeExtent(slice, key, increaseMap);
    }

    return true;
}

void InodeVolumeSpaceDeallocate::DeallocatbleSpaceForVolumeExtent(
    const VolumeExtentSlice &slice, const Key4Inode &key,
    DeallocatableBlockGroupMap *increaseMap) {
    std::unordered_map<uint64_t, bool> relatedBlockGroup;

    for (const auto &next : slice.extents()) {
        // |           BlockGroup         |           BlockGroup          |
        //  -----------                    -----------
        // | 4k bitmap |                  | 4k bitmap |
        //  -----------                    -----------
        // by default, 4K bitmap is not pre-allocated, so the extent will not
        // cross block group.
        auto blockGroupOffset =
            (next.volumeoffset() / blockGroupSize_) * blockGroupSize_;

        auto &exist = (*increaseMap)[blockGroupOffset];
        auto increase = exist.mutable_increase();

        if (relatedBlockGroup.count(blockGroupOffset)) {
            auto oldSize = increase->increasedeallocatablesize();
            increase->set_increasedeallocatablesize(oldSize + next.length());
        } else {
            relatedBlockGroup[blockGroupOffset] = true;
            exist.set_blockgroupoffset(blockGroupOffset);
            increase->set_increasedeallocatablesize(next.length());
        }
    }

    for (const auto &item : relatedBlockGroup) {
        auto &exist = (*increaseMap)[item.first];
        exist.mutable_increase()->add_inodeidlistadd(key.inodeId);
    }
}

// NOTE:
//  1. update the status of DeallocatableBlockGroup: add inodeunderdeallocate
//  and remove related inode to inodeidlist
//
//  2. dallocate the space of the block according to the fetched inode
//
//  3. update the deallocatable status of DeallocatableBlockGroup: decrease
//  deallocatableSize
void InodeVolumeSpaceDeallocate::ProcessSepcifyInodeList(
    uint64_t blockGroupOffset, DeallocatableBlockGroupMap *markMap) {

    // 1
    uint64_t typicalInode =
        (*markMap)[blockGroupOffset].mark().inodeidunderdeallocate(0);
    MetaStatusCode st = executeOpt_.metaClient->UpdateDeallocatableBlockGroup(
        fsId_, typicalInode, markMap);
    if (st != MetaStatusCode::OK) {
        LOG(ERROR) << "InodeVolumeSpaceDeallocate mark inodelist to be "
                      "deallocatable failed, fsId="
                   << fsId_ << ", blockGroupOffset=" << blockGroupOffset;
        return;
    }
    auto &onemark = (*markMap)[blockGroupOffset];

    // 2
    uint64_t decreaseSize = 0;
    auto inodeUnderDeallocate =
        onemark.mutable_mark()->mutable_inodeidunderdeallocate();
    bool ok =
        DeallocateInode(blockGroupOffset, *inodeUnderDeallocate, &decreaseSize);
    if (!ok) {
        LOG(ERROR) << "InodeVolumeSpaceDeallocate an error occurred while "
                      "actually deallocate the volume space, fsId="
                   << fsId_ << ", blockGroupOffset=" << blockGroupOffset;
        return;
    }

    // 3
    DeallocatableBlockGroupMap decrease;
    auto &onedecrease = decrease[blockGroupOffset];
    onedecrease.set_blockgroupoffset(blockGroupOffset);
    onedecrease.mutable_decrease()->set_decreasedeallocatablesize(decreaseSize);
    onedecrease.mutable_decrease()->mutable_inodeddeallocated()->Swap(
        inodeUnderDeallocate);

    st = executeOpt_.metaClient->UpdateDeallocatableBlockGroup(
        fsId_, typicalInode, &decrease);
    if (st != MetaStatusCode::OK) {
        LOG(ERROR) << "InodeVolumeSpaceDeallocate update deallocatable size "
                      "failed, fsId="
                   << fsId_ << ", blockGroupOffset=" << blockGroupOffset;
        return;
    }
}

// NOTE:
// 1. update inode extent
// 2. deallocate the corresponding space in volume
//
// According to this order, one is that the corresponding volume space will not
// be deleted repeatedly, but there is a disadvantage, when the process hangs
// up, this part of the space will not be recovered. And may need to be
// deallocate by means of fscheck, etc.
bool InodeVolumeSpaceDeallocate::DeallocateInode(uint64_t blockGroupOffset,
                                                 const Uint64Vec &inodelist,
                                                 uint64_t *decrease) {
    std::vector<Extent> deallocatableVolumeSpace;

    for (const auto &inodeId : inodelist) {
        VolumeExtentSliceList sliceList;
        MetaStatusCode st = calOpt_.inodeStorage->GetAllVolumeExtent(
            fsId_, inodeId, &sliceList);
        if (st != MetaStatusCode::OK) {
            LOG(ERROR) << "InodeVolumeSpaceDeallocate get inode extent "
                          "failed, fsId="
                       << fsId_ << ", inodeId=" << inodeId;
            return false;
        }

        // 1
        UpdateDeallocateInodeExtentSlice(blockGroupOffset, inodeId, decrease,
                                         &sliceList, &deallocatableVolumeSpace);
        auto closure = new UpdateVolumeExtentClosure(nullptr, true);
        executeOpt_.metaClient->AsyncUpdateVolumeExtent(fsId_, inodeId,
                                                        sliceList, closure);
        closure->Wait();
        // TODO(ilixiaocui): AsyncUpdateVolumeExtent update failed
    }

    // 2
    bool ok = executeOpt_.volumeSpaceManager->DeallocVolumeSpace(
        fsId_, blockGroupOffset, deallocatableVolumeSpace);
    if (!ok) {
        // TODO(ilixiaocui): this part of the non-recyclable space should be
        // included in the metric statistics
    }

    return ok;
}

void InodeVolumeSpaceDeallocate::UpdateDeallocateInodeExtentSlice(
    uint64_t blockGroupOffset, uint64_t inodeId, uint64_t *decrease,
    VolumeExtentSliceList *sliceList,
    std::vector<Extent> *deallocatableVolumeSpace) {
    uint64_t boundary =
        (blockGroupOffset / blockGroupSize_ + 1) * blockGroupSize_;

    for (auto &slice : *sliceList->mutable_slices()) {
        auto extents = slice.mutable_extents();
        extents->erase(
            std::remove_if(extents->begin(), extents->end(),
                           [&](const VolumeExtent &extent) {
                               bool res = (extent.volumeoffset() >=
                                           blockGroupOffset) &&
                                          (extent.volumeoffset() < boundary);
                               if (res) {
                                   *decrease += extent.length();
                                   deallocatableVolumeSpace->emplace_back(
                                       extent.volumeoffset(), extent.length());
                               }
                               return res;
                           }),
            extents->end());
    }
}

}  // namespace metaserver
}  // namespace curvefs

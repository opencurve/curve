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
 * Date: Wed Mar 22 10:39:04 CST 2023
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_METASERVER_SPACE_INODE_VOLUME_SPACE_DEALLOCATE_H_
#define CURVEFS_SRC_METASERVER_SPACE_INODE_VOLUME_SPACE_DEALLOCATE_H_

#include <gtest/gtest_prod.h>

#include <map>
#include <string>
#include <vector>
#include <utility>
#include <memory>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/common.pb.h"
#include "curvefs/src/metaserver/storage/converter.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/space/volume_space_manager.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/src/volume/common.h"
#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "curvefs/src/metaserver/metacli_manager.h"

namespace curvefs {
namespace metaserver {

using curvefs::client::rpcclient::MetaServerClient;
using curvefs::common::BlockGroupID;
using curvefs::metaserver::storage::Converter;
using curvefs::metaserver::storage::Key4DeallocatableBlockGroup;
using curvefs::metaserver::storage::Key4Inode;
using curvefs::metaserver::storage::KVStorage;
using curvefs::metaserver::storage::Prefix4InodeVolumeExtent;
using curvefs::volume::Extent;

using Uint64Vec = google::protobuf::RepeatedField<google::protobuf::uint64>;
using DeallocatableBlockGroupMap = std::map<uint64_t, DeallocatableBlockGroup>;

struct VolumeDeallocateCalOption {
    Converter conv;
    std::shared_ptr<NameGenerator> nameGen;
    std::shared_ptr<KVStorage> kvStorage;
    std::shared_ptr<InodeStorage> inodeStorage;
};

struct VolumeDeallocateExecuteOption {
    std::shared_ptr<VolumeSpaceManager> volumeSpaceManager;
    std::shared_ptr<MetaServerClient> metaClient;

    uint32_t batchClean;
};

class InodeVolumeSpaceDeallocate {
 public:
    InodeVolumeSpaceDeallocate(
        uint64_t fsId, uint32_t partitionId,
        std::shared_ptr<copyset::CopysetNode> copysetNode)
        : fsId_(fsId), partitionId_(partitionId), copysetNode_(copysetNode) {
        metaCli_ = MetaCliManager::GetInstance().GetMetaCli(fsId_);
    }

    void Init(VolumeDeallocateCalOption calOpt) { calOpt_ = std::move(calOpt); }

    int Init(const VolumeDeallocateExecuteOption &executeOpt) {
        executeOpt_.volumeSpaceManager = executeOpt.volumeSpaceManager;
        executeOpt_.metaClient = executeOpt.metaClient;
        executeOpt_.batchClean = executeOpt.batchClean;

        blockGroupSize_ =
            executeOpt_.volumeSpaceManager->GetBlockGroupSize(fsId_);
        if (blockGroupSize_ <= 0) {
            return -1;
        }

        LOG(INFO) << "InodeVolumeSpaceDeallocate init, fsid=" << fsId_
                  << ", partitionId=" << partitionId_
                  << ", blockGroupSize=" << blockGroupSize_;
        return 0;
    }

    // used to traverse the list of inodes to be deleted, and count the
    // deallocatable space of BlockGroup
    void CalDeallocatableSpace();

    // Return the reclaimable space of each BlockGroup
    //  deallocatablespace: key=str(fsid+offset) value=(deallocatabe space size)
    void
    GetDeallocatableSpace(std::map<std::string, uint64_t> *deallocatablespaces);

    // DeAllocate the space of the specified BlockGroup
    MetaStatusCode DeallocateOneBlockGroup(uint64_t blockGroupOffset);

    uint32_t GetPartitionID() const { return partitionId_; }

    void SetCanceled() { canceled_ = true; }

    bool IsCanceled() const { return canceled_; }

    bool HasDeallocateTask() {
        return waitingBlockGroupOffset_.has_value();
    }

    uint64_t GetDeallocateTask() {
        return waitingBlockGroupOffset_.value();
    }

    void ResetDeallocateTask() {
        waitingBlockGroupOffset_.reset();
     }

    void SetDeallocateTask(uint64_t blockGroupOffset) {
        waitingBlockGroupOffset_ = blockGroupOffset;
        LOG(INFO) << "InodeVolumeSpaceDeallocate set deallocate task, "
                  << "blockGroupOffset=" << blockGroupOffset;
    }

    bool CanStart() {
        bool leader = copysetNode_->IsLeaderTerm();
        if (!leader) {
            LOG(WARNING) << "InodeVolumeSpaceDeallocate copyset="
                         << copysetNode_->Name() << " is not leader, skip";
        }
        return leader;
    }

 private:
    friend class InodeVolumeSpaceDeallocateTest;
    FRIEND_TEST(InodeVolumeSpaceDeallocateTest,
                Test_DeallocatableSapceForInode);
    FRIEND_TEST(InodeVolumeSpaceDeallocateTest, Test_ProcessSepcifyInodeList);
    FRIEND_TEST(InodeVolumeSpaceDeallocateTest, Test_DeallocateInode);
    FRIEND_TEST(InodeVolumeSpaceDeallocateTest,
                Test_UpdateDeallocateInodeExtentSlice);

    bool DeallocatableSapceForInode(const Key4Inode &key,
                                    DeallocatableBlockGroupMap *increaseMap);

    void
    DeallocatbleSpaceForVolumeExtent(const VolumeExtentSlice &slice,
                                     const Key4Inode &key,
                                     DeallocatableBlockGroupMap *increaseMap);

    void ProcessSepcifyInodeList(uint64_t blockGroupOffset,
                                 DeallocatableBlockGroupMap *markMap);
    bool DeallocateInode(uint64_t blockGroupOffset, const Uint64Vec &inodelist,
                         uint64_t *decrease);

 private:
    void UpdateDeallocateInodeExtentSlice(
        uint64_t blockGroupOffset, uint64_t inodeId, uint64_t *decrease,
        VolumeExtentSliceList *sliceList,
        std::vector<Extent> *deallocatableVolumeSpace);

 private:
    VolumeDeallocateCalOption calOpt_;
    VolumeDeallocateExecuteOption executeOpt_;

    uint64_t fsId_;
    uint32_t partitionId_;
    uint64_t blockGroupSize_;

    absl::optional<uint64_t> waitingBlockGroupOffset_;

    bool canceled_{false};

    std::shared_ptr<copyset::CopysetNode> copysetNode_;
    std::shared_ptr<MetaServerClient> metaCli_;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_SPACE_INODE_VOLUME_SPACE_DEALLOCATE_H_

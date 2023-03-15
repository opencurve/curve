/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Friday Feb 25 17:45:46 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_MDS_SPACE_VOLUME_SPACE_H_
#define CURVEFS_SRC_MDS_SPACE_VOLUME_SPACE_H_

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <gtest/gtest_prod.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/heartbeat.pb.h"
#include "curvefs/src/mds/fs_storage.h"
#include "curvefs/src/mds/space/block_group_storage.h"
#include "src/common/interruptible_sleeper.h"

namespace curvefs {
namespace mds {
namespace space {

using ::curvefs::mds::heartbeat::BlockGroupDeallcateStatusCode;
using ::curvefs::metaserver::DeallocatableBlockGroup;
using ::curve::common::InterruptibleSleeper;

using DeallocatableBlockGroupVec =
    google::protobuf::RepeatedPtrField<DeallocatableBlockGroup>;
using BlockGroupDeallcateStatusMap = ::google::protobuf::Map<
    ::google::protobuf::uint64,
    ::curvefs::mds::heartbeat::BlockGroupDeallcateStatusCode>;

class AbstractVolumeSpace {
 public:
    virtual ~AbstractVolumeSpace() = default;

    virtual SpaceErrCode AllocateBlockGroups(
        uint32_t count,
        const std::string& owner,
        std::vector<BlockGroup>* blockGroups) = 0;

    virtual SpaceErrCode AcquireBlockGroup(uint64_t blockGroupOffset,
                                           const std::string& owner,
                                           BlockGroup* group) = 0;

    virtual SpaceErrCode ReleaseBlockGroups(
        const std::vector<BlockGroup>& blockGroups) = 0;

    virtual bool
    UpdateDeallocatableBlockGroup(uint32_t metaserverId, uint32_t metaserverNum,
                                  const DeallocatableBlockGroupVec &groups,
                                  const BlockGroupDeallcateStatusMap &stats,
                                  uint64_t *issue) = 0;
};

using ::curvefs::common::BitmapLocation;
using ::curvefs::common::Volume;

class VolumeSpace final : public AbstractVolumeSpace {
 public:
    static std::unique_ptr<VolumeSpace>
    Create(uint32_t fsId, const Volume &volume, BlockGroupStorage *storage,
           FsStorage *fsStorage, uint64_t calcIntervalSec);

    VolumeSpace(const VolumeSpace&) = delete;
    VolumeSpace& operator=(const VolumeSpace&) = delete;
    ~VolumeSpace() {}

    /**
     * @brief Allocate block groups
     */
    SpaceErrCode AllocateBlockGroups(
        uint32_t count,
        const std::string& owner,
        std::vector<BlockGroup>* blockGroups) override;

    /**
     * @brief Acquire a designated block group by offset
     */
    SpaceErrCode AcquireBlockGroup(uint64_t blockGroupOffset,
                                   const std::string& owner,
                                   BlockGroup* group) override;

    /**
     * @brief Release block groups
     */
    SpaceErrCode ReleaseBlockGroups(
        const std::vector<BlockGroup>& blockGroups) override;

    /**
     * @brief Remove all block groups and persistent records that belong to
     *        current volume
     * @return return SpaceOk if success, otherwise return error code
     */
    SpaceErrCode RemoveAllBlockGroups();


    bool
    UpdateDeallocatableBlockGroup(uint32_t metaserverId, uint32_t metaserverNum,
                                  const DeallocatableBlockGroupVec &groups,
                                  const BlockGroupDeallcateStatusMap &stats,
                                  uint64_t *issue);

    /**
     * @brief Calculate block group that can be recycled
     */
    void Run();

    void Stop();

 private:
    VolumeSpace(uint32_t fsId,
                Volume volume,
                BlockGroupStorage* storage,
                FsStorage* fsStorage);

 private:
    SpaceErrCode AllocateBlockGroupsInternal(
        uint32_t count,
        const std::string& owner,
        std::vector<BlockGroup>* blockGroups);

    uint32_t AllocateFromAvailableGroups(uint32_t count,
                                         const std::string& owner,
                                         std::vector<BlockGroup>* groups);

    uint32_t AllocateFromCleanGroups(uint32_t count,
                                     const std::string& owner,
                                     std::vector<BlockGroup>* groups);

    SpaceErrCode AcquireBlockGroupInternal(uint64_t blockGroupOffset,
                                           const std::string& owner,
                                           BlockGroup* group);

    SpaceErrCode ExtendVolume();

    bool UpdateFsInfo(uint64_t origin, uint64_t extended);

    void AddCleanGroups(uint64_t origin, uint64_t extended);

    // pick out blockgroups that can be authorized for metaserver processing
    void CalBlockGroupAvailableForDeAllocate();

    // update the deallocatable space of blockgroup
    void UpdateBlockGroupDeallocatableSpace(
        uint32_t metaserverId, const DeallocatableBlockGroupVec &groups);

    // update the deallocating progress reported by the metaserver
    void
    UpdateDeallocatingBlockGroup(uint32_t metaserverId,
                                 const BlockGroupDeallcateStatusMap &stats);

    // check whether there is currently a blockgroup that can be recycled and
    // send it to the metaserver
    bool SelectBlockGroupForDeAllocate(uint32_t metaserverId, uint64_t *issue);

    FRIEND_TEST(VolumeSpaceTest, Test_CalBlockGroupAvailableForDeAllocate);

 private:
    // persist block group to backend storage
    SpaceErrCode PersistBlockGroup(const BlockGroup& group);
    SpaceErrCode PersistBlockGroups(const std::vector<BlockGroup>& blockGroups);

    // remove corresponding block group from backend storage
    SpaceErrCode ClearBlockGroup(const BlockGroup& group);

 private:
    FRIEND_TEST(VolumeSpaceTest, TestAutoExtendVolumeSuccess);

    const uint32_t fsId_;
    Volume volume_;

    mutable bthread::Mutex mtx_;

    // block groups are divided into three types
    // 1. allocated
    //    these block groups are now owned by clients(curve-fuse or
    //    curvefs-metaserver), these block groups are forbidden to reallocate to
    //    other clients.
    //    allocated block groups are persisted into storage.
    // 2. available
    //    these block groups had been used by some clients, but now they don't
    //    have owners, and their space is partial used. so they can be
    //    reallocated to other clients.
    //    available block groups are persisted into storage, but doesn't have to
    //    do this, we persist these block groups for two reasons, first, we can
    //    statistics space usage more convenient, second is we can reallocate
    //    these block groups to its previous owner.
    // 3. clean
    //    these block groups' space is never used, and they can be allocated to
    //    other clients.
    // 4. waitDeallocate
    //    these block groups wait for being deallocated by
    //    metaservers, and they can not allocate to clients.
    // 5. deallocating
    //    these block groups are being deallocated by metaservers, and they can
    //    not allocate to clients.
    //    these block groups are not persisted into storage.

    // key is block group offset
    std::unordered_map<uint64_t, BlockGroup> allocatedGroups_;

    // key is block group offset
    std::unordered_map<uint64_t, BlockGroup> availableGroups_;

    // stores clean block groups' offset
    std::unordered_set<uint64_t> cleanGroups_;

    // key is block group offset
    std::unordered_map<uint64_t, BlockGroup> waitDeallocateGroups_;

    // key is block group offset
    std::unordered_map<uint64_t, BlockGroup> deallocatingGroups_;

    mutable bthread::Mutex statmtx_;
    // Summarize the deallocatable space of the blockgroup in this volume
    // reported by all metaservers
    //
    // The metaserver reports the full amount of information instead of
    // incremental information, so adding this data to the statistical data
    // requires subtracting the last data
    //
    // - lastupdate_
    //   metaserver report last time
    // - summary_
    //   summary add metaserver report info and remove lastupdate info

    // key is metaserver id
    std::unordered_map<uint32_t, DeallocatableBlockGroupVec> lastUpdate_;

    // key is block group offset, value is deallocatable size
    std::unordered_map<uint64_t, uint64_t> summary_;

    BlockGroupStorage* storage_;

    FsStorage* fsStorage_;

    std::thread calThread_;
    InterruptibleSleeper sleeper_;
    int64_t calcIntervalSec_;

    std::atomic<uint32_t> metaserverNum_;

 private:
    struct Metric {
        bvar::Adder<uint64_t> dealloc;
        bvar::Adder<uint64_t> waitingDealloc;

        Metric()
            : dealloc("mds_volume_space_dealloc"),
              waitingDealloc("mds_volume_space_wait_dealloc") {}
    };

    Metric metric_;
};

// Calculate extended size based on origin with factor, and the r esult size is
// aligned with alignment
uint64_t ExtendedSize(uint64_t origin, double factor, uint64_t alignment);

}  // namespace space
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SPACE_VOLUME_SPACE_H_

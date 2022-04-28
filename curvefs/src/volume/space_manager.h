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
 * Date: Wednesday Mar 02 19:56:23 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_VOLUME_SPACE_MANAGER_H_
#define CURVEFS_SRC_VOLUME_SPACE_MANAGER_H_

#include <map>
#include <memory>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/volume/allocator.h"
#include "curvefs/src/volume/block_device_client.h"
#include "curvefs/src/volume/block_group_manager.h"
#include "curvefs/src/volume/common.h"
#include "src/common/concurrent/rw_lock.h"

namespace curvefs {
namespace volume {

using ::curvefs::client::rpcclient::MdsClient;

class SpaceManager {
 public:
    virtual ~SpaceManager() = default;

    virtual bool Alloc(uint32_t size,
                       const AllocateHint& hint,
                       std::vector<Extent>* extents) = 0;

    virtual bool DeAlloc(const std::vector<Extent>& extents) = 0;

    virtual bool Shutdown() = 0;
};

class SpaceManagerImpl final : public SpaceManager {
 public:
    SpaceManagerImpl(
        const SpaceManagerOption& option,
        const std::shared_ptr<MdsClient>& mdsClient,
        const std::shared_ptr<BlockDeviceClient>& blockDeviceClient);

    SpaceManagerImpl(const SpaceManagerImpl&) = delete;
    SpaceManagerImpl& operator=(const SpaceManagerImpl&) = delete;

    bool Alloc(uint32_t size,
               const AllocateHint& hint,
               std::vector<Extent>* extents) override;

    bool DeAlloc(const std::vector<Extent>& extents) override;

    /**
     * @brief Shutdown space manager, release all blockgroups' rights
     */
    bool Shutdown() override;

 private:
    int64_t AllocInternal(int64_t size,
                          const AllocateHint& hint,
                          std::vector<Extent>* exts);

    std::map<uint64_t, std::unique_ptr<Allocator>>::iterator FindAllocator(
        const AllocateHint& hint);

    /**
     * @brief Find corresponding bitmap updater by extent
     */
    BlockGroupBitmapUpdater* FindBitmapUpdater(const Extent& ext);

    bool UpdateBitmap(const std::vector<Extent>& exts);

 private:
    bool AllocateBlockGroup(uint64_t hint);

    bool AcquireBlockGroup(uint64_t blockGroupOffset);

 private:
    curve::common::RWLock allocatorsLock_;
    std::map<uint64_t, std::unique_ptr<Allocator>> allocators_;

    curve::common::RWLock updatersLock_;
    std::map<uint64_t, std::unique_ptr<BlockGroupBitmapUpdater>>
        bitmapUpdaters_;

    std::atomic<uint64_t> totalBytes_;
    std::atomic<uint64_t> availableBytes_;

    // from fileinfo
    uint32_t blockSize_;
    uint64_t blockGroupSize_;

    std::unique_ptr<BlockGroupManager> blockGroupManager_;

    bool allocating_;
    std::mutex mtx_;
    std::condition_variable cond_;

 private:
    struct Metric {
        bvar::LatencyRecorder allocLatency;
        bvar::LatencyRecorder allocSize;
        bvar::Adder<uint64_t> errorCount;

        Metric()
            : allocLatency("space_alloc_latency"),
              allocSize("space_alloc_size"),
              errorCount("space_alloc_error") {}
    };

    Metric metric_;
};

}  // namespace volume
}  // namespace curvefs

#endif  // CURVEFS_SRC_VOLUME_SPACE_MANAGER_H_

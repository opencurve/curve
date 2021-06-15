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

/**
 * Project: curve
 * File Created: Fri Jul 16 21:22:40 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_SPACE_SPACE_MANAGER_H_
#define CURVEFS_SRC_SPACE_SPACE_MANAGER_H_

#include <memory>
#include <unordered_map>

#include "curvefs/proto/space.pb.h"
#include "curvefs/src/space/allocator.h"
#include "curvefs/src/space/config.h"
#include "src/common/concurrent/rw_lock.h"

namespace curvefs {
namespace space {

class SpaceManager {
 public:
    SpaceManager() = default;

    virtual ~SpaceManager() = default;

    SpaceManager(const SpaceManager&) = delete;

    SpaceManager& operator=(const SpaceManager&) = delete;

    virtual SpaceStatusCode InitSpace(const mds::FsInfo& fsInfo) = 0;

    virtual SpaceStatusCode UnInitSpace(uint32_t fsId) = 0;

    virtual SpaceStatusCode StatSpace(uint32_t fsId, SpaceStat* stat) = 0;

    virtual SpaceStatusCode AllocateSpace(uint32_t fsId, uint64_t size,
                                          const SpaceAllocateHint& hint,
                                          Extents* exts) = 0;

    virtual SpaceStatusCode DeallocateSpace(uint32_t fsId,
                                            const ProtoExtents& extents) = 0;
};

class SpaceManagerImpl : public SpaceManager {
 public:
    explicit SpaceManagerImpl(const SpaceManagerOption& opt);

    SpaceStatusCode InitSpace(const mds::FsInfo& fsInfo) override;

    SpaceStatusCode UnInitSpace(uint32_t fsId) override;

    SpaceStatusCode StatSpace(uint32_t fsId, SpaceStat* stat) override;

    SpaceStatusCode AllocateSpace(uint32_t fsId, uint64_t size,
                                  const SpaceAllocateHint& hint,
                                  Extents* exts) override;

    SpaceStatusCode DeallocateSpace(uint32_t fsId,
                                    const ProtoExtents& extents) override;

 private:
    SpaceManagerOption opt_;

    ::curve::common::BthreadRWLock blkRwlock_;

    std::unordered_map<uint32_t, std::unique_ptr<Allocator>> allocators_;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_SPACE_MANAGER_H_

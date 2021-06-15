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

#ifndef CURVEFS_SRC_SPACE_ALLOCATOR_FREE_EXTENTS_H_
#define CURVEFS_SRC_SPACE_ALLOCATOR_FREE_EXTENTS_H_

#include <cstdint>
#include <map>
#include <vector>

#include "curvefs/proto/space.pb.h"
#include "curvefs/src/space_allocator/common.h"

namespace curvefs {
namespace space {

std::ostream& operator<<(std::ostream& os, const PExtent& e);
std::ostream& operator<<(std::ostream& os, const std::vector<PExtent>& exts);

// Two roles:
//   1. As a small space allocator, the initial state maintains a space
//   2. As a secondary allocator of large block space allocation, the initial state does not maintain space  // NOLINT
class FreeExtents {
 public:
    // Role 1
    FreeExtents(uint64_t off, uint64_t len);

    // Role 2
    explicit FreeExtents(uint64_t maxExtentSize);

    /**
     * @brief DeAllocate space
     */
    void DeAlloc(uint64_t off, uint64_t len) {
        assert(len > 0);

        DeAllocInternal(off, len);
        available_ += len;

        assert(maxLength_ == 0 ||
               (maxLength_ != 0 && available_ <= maxLength_));
    }

    /**
     * @brief Allocate space
     */
    uint64_t Alloc(uint64_t size, const SpaceAllocateHint& hint,
                   std::vector<PExtent>* exts) {
        auto alloc = AllocInternal(size, hint, exts);
        assert(available_ >= alloc);
        available_ -= alloc;

        return alloc;
    }

    /**
     * @brief Mark extent: [off, len] used
     */
    void MarkUsed(uint64_t off, uint64_t len) {
        MarkUsedInternal(off, len);
        assert(available_ >= len);
        available_ -= len;
    }

    uint64_t AvailableSize() const {
        return available_;
    }

    /**
     * @brief Get current available extents
     */
    ExtentMapT AvailableExtents() const {
        return extents_;
    }

    /**
     * @brief Get currnet available blocks
     * @return Total size of available blocks
     */
    uint64_t AvailableBlocks(ExtentMapT* blocks);

    friend std::ostream& operator<<(std::ostream& os, const FreeExtents& e);

 private:
    uint64_t AllocInternal(uint64_t size, const SpaceAllocateHint& hint,
                           std::vector<PExtent>* exts);

    void DeAllocInternal(uint64_t off, uint64_t len);

    void MarkUsedInternal(uint64_t off, uint64_t len);

 private:
    FreeExtents(const FreeExtents&);
    FreeExtents& operator=(const FreeExtents&);

 private:
    const uint64_t maxLength_;
    const uint64_t maxExtentSize_;
    uint64_t available_;

    ExtentMapT extents_;
    ExtentMapT blocks_;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_ALLOCATOR_FREE_EXTENTS_H_

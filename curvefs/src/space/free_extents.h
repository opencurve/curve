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

#ifndef CURVEFS_SRC_SPACE_FREE_EXTENTS_H_
#define CURVEFS_SRC_SPACE_FREE_EXTENTS_H_

#include <cstdint>

#include "curvefs/src/space/common.h"

namespace curvefs {
namespace space {

// Two roles:
//   1. As a small space allocator, the initial state maintains a space.
//      Alloc/DeAlloc are used for allocate space and recycle space
//   2. As a secondary allocator of large block space allocation,
//      the initial state does not maintain space.
//      Alloc is used for allocate space
//      DeAlloc has two purpose:
//        recycle space
//        store smaller space from BitmapAllocator that doesn't satisfy `maxExtentSize`  // NOLINT
class FreeExtents {
 public:
    // Role 1
    FreeExtents(const uint64_t off, const uint64_t len);

    // Role 2
    explicit FreeExtents(const uint64_t maxExtentSize);

    FreeExtents(const FreeExtents&) = delete;

    FreeExtents& operator=(const FreeExtents&) = delete;

    /**
     * @brief DeAllocate space
     */
    void DeAlloc(const uint64_t off, const uint64_t len) {
        DeAllocInternal(off, len);
        available_ += len;
    }

    /**
     * @brief Allocate space
     */
    uint64_t Alloc(const uint64_t size, const SpaceAllocateHint& hint,
                   Extents* exts) {
        auto alloc = AllocInternal(size, hint, exts);
        available_ -= alloc;

        return alloc;
    }

    /**
     * @brief Mark extent: [off, len] used
     */
    void MarkUsed(const uint64_t off, const uint64_t len) {
        MarkUsedInternal(off, len);
        available_ -= len;
    }

    uint64_t AvailableSize() const {
        return available_;
    }

    /**
     * @brief Get current available extents
     */
    const ExtentMap& AvailableExtents() const {
        return extents_;
    }

    /**
     * @brief Get currnet available blocks
     * @return Total size of available blocks
     */
    uint64_t AvailableBlocks(ExtentMap* blocks);

    friend std::ostream& operator<<(std::ostream& os, const FreeExtents& e);

 private:
    uint64_t AllocInternal(uint64_t size, const SpaceAllocateHint& hint,
                           Extents* exts);

    void DeAllocInternal(const uint64_t off, const uint64_t len);

    void MarkUsedInternal(const uint64_t off, const uint64_t len);

 private:
    const uint64_t maxLength_;
    const uint64_t maxExtentSize_;
    uint64_t available_;

    ExtentMap extents_;
    ExtentMap blocks_;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_FREE_EXTENTS_H_

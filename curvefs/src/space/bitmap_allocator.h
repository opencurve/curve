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

#ifndef CURVEFS_SRC_SPACE_BITMAP_ALLOCATOR_H_
#define CURVEFS_SRC_SPACE_BITMAP_ALLOCATOR_H_

#include <bthread/mutex.h>

#include "curvefs/src/space/allocator.h"
#include "curvefs/src/space/common.h"
#include "curvefs/src/space/config.h"
#include "curvefs/src/space/free_extents.h"
#include "src/common/bitmap.h"

namespace curvefs {
namespace space {

class BitmapAllocator : public Allocator {
 public:
    explicit BitmapAllocator(const BitmapAllocatorOption& opt);

    ~BitmapAllocator();

    uint64_t Alloc(const uint64_t size, const SpaceAllocateHint& hint,
                   Extents* exts) override;

    bool DeAlloc(const uint64_t off, const uint64_t len) override;

    bool DeAlloc(const Extents& exts) override;

    uint64_t StartOffset() const {
        return opt_.startOffset;
    }

    uint64_t Total() const override {
        return opt_.length;
    }

    uint64_t AvailableSize() const override {
        std::lock_guard<bthread::Mutex> lock(mtx_);
        return available_;
    }

    bool MarkUsed(const Extents& extents) override;

    bool MarkUsable(const Extents& extents) override;

    static uint64_t CalcBitmapAreaLength(const BitmapAllocatorOption& opt);

    friend std::ostream& operator<<(std::ostream& os,
                                    const BitmapAllocator& alloc);

 private:
    struct AllocOrder;

    /**
     * @brief call real allocate functions that stored in AllocOrder one by one,
     * untilsatisfy size or all functions has been called
     */
    uint64_t AllocInternal(const AllocOrder& order, const uint64_t size,
                           const SpaceAllocateHint& hint, Extents* exts);

    // allocate staffs
    uint64_t AllocFromBitmap(const uint64_t size, const SpaceAllocateHint& hint,
                             Extents* exts);
    uint64_t AllocFromBitmapExtent(const uint64_t size,
                                   const SpaceAllocateHint& hint,
                                   Extents* exts);
    uint64_t AllocFromSmallExtent(const uint64_t size,
                                  const SpaceAllocateHint& hint, Extents* exts);

    // deallocate staffs
    void DeAllocToSmallExtent(const uint64_t off, const uint64_t len);
    void DeAllocToBitmap(const uint64_t off, const uint64_t len);
    void DeAllocToBitmapExtent(const uint64_t off, const uint64_t len);

    // mark space[off,len] is used
    void MarkUsedInternal(const uint64_t off, const uint64_t len);
    void MarkUsedForSmallExtent(const uint64_t off, const uint64_t len);
    void MarkUsedForBitmap(const uint64_t off, const uint64_t len);

    // split off and len by bitmapAreaOffset
    void Split(const uint64_t off, const uint64_t len,
               uint64_t* offInSmallExtent, uint64_t* lenInSmallExtent,
               uint64_t* offInBitmap, uint64_t* lenInBitmap) const;

 private:
    const BitmapAllocatorOption opt_;
    const uint64_t bitmapAreaLength_;
    const uint64_t smallAreaLength_;
    const uint64_t bitmapAreaOffset_;

    // protect below fields
    mutable bthread::Mutex mtx_;

    // current available size
    uint64_t available_;

    // last bitmap allocate index
    uint64_t bitmapAllocIdx_;

    // space bitmap, 1 means used
    curve::common::Bitmap bitmap_;

    // unused extents of bitmap
    FreeExtents bitmapExtent_;

    // for small size allocate
    FreeExtents smallExtent_;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_BITMAP_ALLOCATOR_H_

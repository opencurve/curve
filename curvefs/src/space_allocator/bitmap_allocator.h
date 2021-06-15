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

#ifndef CURVEFS_SRC_SPACE_ALLOCATOR_BITMAP_ALLOCATOR_H_
#define CURVEFS_SRC_SPACE_ALLOCATOR_BITMAP_ALLOCATOR_H_

#include <bthread/mutex.h>

#include <vector>

#include "curvefs/src/space_allocator/allocator.h"
#include "curvefs/src/space_allocator/bitmap.h"
#include "curvefs/src/space_allocator/common.h"
#include "curvefs/src/space_allocator/free_extents.h"

namespace curvefs {
namespace space {

struct BitmapAllocatorOption {
    uint64_t startOffset;
    uint64_t length;
    uint64_t sizePerBit;
    double smallAllocProportion;
};

// 管理一段空间的分配
class BitmapAllocator : public Allocator {
 public:
    explicit BitmapAllocator(const BitmapAllocatorOption& opt);

    ~BitmapAllocator();

    uint64_t Alloc(uint64_t size, const SpaceAllocateHint& hint,
                   std::vector<PExtent>* exts) override;

    void DeAlloc(uint64_t off, uint64_t len) override;

    void DeAlloc(const std::vector<PExtent>& exts) override;

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

    bool MarkUsed(const std::vector<PExtent>& extents) override;

    bool MarkUsable(const std::vector<PExtent>& extents) override;

 private:
    uint64_t AllocSmall(uint64_t size, const SpaceAllocateHint& hint,
                        std::vector<PExtent>* exts);
    uint64_t AllocBig(uint64_t size, const SpaceAllocateHint& hint,
                      std::vector<PExtent>* exts);

    uint64_t AllocFromBitmap(uint64_t size, const SpaceAllocateHint& hint,
                             std::vector<PExtent>* exts);
    uint64_t AllocFromBitmapExtent(uint64_t size, const SpaceAllocateHint& hint,
                                   std::vector<PExtent>* exts);
    uint64_t AllocFromSmallExtent(uint64_t size, const SpaceAllocateHint& hint,
                                  std::vector<PExtent>* exts);

    void DeAllocToSmallExtent(uint64_t off, uint64_t len);
    void DeAllocToBitmap(uint64_t off, uint64_t len);
    void DeAllocToBitmapExtent(uint64_t off, uint64_t len);

    void MarkUsedInternal(uint64_t off, uint64_t len);
    void MarkUsedForSmallExtent(uint64_t off, uint64_t len);
    void MarkUsedForBitmap(uint64_t off, uint64_t len);

    void Split(uint64_t off, uint64_t len, uint64_t* offInSmallExtent,
               uint64_t* lenInSmallExtent, uint64_t* offInBitmap,
               uint64_t* lenInBitmap) const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const BitmapAllocator& alloc);

 private:
    static uint64_t CalcBitmapAreaLength(const BitmapAllocatorOption& opt);

 private:
    const BitmapAllocatorOption opt_;
    const uint64_t bitmapAreaLength_;
    const uint64_t smallAreaLength_;
    const uint64_t bitmapAreaOffset_;

    mutable bthread::Mutex mtx_;

    uint64_t available_;
    uint64_t bitmapAllocIdx_;

    Bitmap bitmap_;
    FreeExtents bitmapExtent_;
    FreeExtents smallExtent_;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_ALLOCATOR_BITMAP_ALLOCATOR_H_

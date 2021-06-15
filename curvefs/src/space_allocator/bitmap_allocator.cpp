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

#include "curvefs/src/space_allocator/bitmap_allocator.h"

#include <glog/logging.h>
#include <vector>

#include <algorithm>

#include "curvefs/src/space_allocator/bitmap.h"

namespace curvefs {
namespace space {

BitmapAllocator::BitmapAllocator(const BitmapAllocatorOption& opt)
    : opt_(opt),
      bitmapAreaLength_(CalcBitmapAreaLength(opt)),
      smallAreaLength_(opt_.length - bitmapAreaLength_),
      bitmapAreaOffset_(opt_.startOffset + smallAreaLength_),
      mtx_(),
      available_(opt_.length),
      bitmapAllocIdx_(0),
      bitmap_(bitmapAreaLength_ / opt_.sizePerBit),
      bitmapExtent_(opt_.sizePerBit),
      smallExtent_(opt_.startOffset, smallAreaLength_) {
    assert(bitmap_.Size() * opt_.sizePerBit + smallExtent_.AvailableSize() ==
           opt_.length);
}

BitmapAllocator::~BitmapAllocator() {}

uint64_t BitmapAllocator::Alloc(uint64_t size, const SpaceAllocateHint& hint,
                                std::vector<PExtent>* exts) {
    std::lock_guard<bthread::Mutex> lock(mtx_);
    if (available_ == 0) {
        return 0;
    }

    uint64_t alloc = 0;

    if (size < opt_.sizePerBit && (hint.allocType == AllocateType::NONE ||
                                   hint.allocType == AllocateType::SMALL)) {
        alloc = AllocSmall(size, hint, exts);
    } else {
        alloc = AllocBig(size, hint, exts);
    }

    assert(alloc <= available_);
    available_ -= alloc;

    return alloc;
}

void BitmapAllocator::DeAlloc(uint64_t off, uint64_t len) {
    std::lock_guard<bthread::Mutex> lock(mtx_);

    uint64_t offInSmallExtent = 0;
    uint64_t lenInSmallExtent = 0;
    uint64_t offInBitmap = 0;
    uint64_t lenInBitmap = 0;

    Split(off, len, &offInSmallExtent, &lenInSmallExtent, &offInBitmap,
          &lenInBitmap);

    if (lenInSmallExtent != 0) {
        DeAllocToSmallExtent(offInSmallExtent, lenInSmallExtent);
    }

    if (lenInBitmap != 0) {
        DeAllocToBitmap(offInBitmap, lenInBitmap);
    }

    assert(available_ + len <= opt_.length);

    available_ += len;
}

void BitmapAllocator::DeAlloc(const std::vector<PExtent>& exts) {
    for (const auto& e : exts) {
        DeAlloc(e.offset, e.len);
    }
}

bool BitmapAllocator::MarkUsed(const std::vector<PExtent>& extents) {
    for (auto& e : extents) {
        MarkUsedInternal(e.offset, e.len);
    }

    return true;
}

bool BitmapAllocator::MarkUsable(const std::vector<PExtent>& extents) {
    return false;
}

uint64_t BitmapAllocator::AllocSmall(uint64_t size,
                                     const SpaceAllocateHint& hint,
                                     std::vector<PExtent>* exts) {
    uint64_t need = size;
    need -= AllocFromSmallExtent(need, hint, exts);
    if (need == 0) {
        return size;
    }

    need -= AllocFromBitmapExtent(need, hint, exts);
    if (need == 0) {
        return size;
    }

    need -= AllocFromBitmap(need, hint, exts);
    return size - need;
}

uint64_t BitmapAllocator::AllocBig(uint64_t size, const SpaceAllocateHint& hint,
                                   std::vector<PExtent>* exts) {
    uint64_t need = size;
    auto allocated = AllocFromBitmap(need, hint, exts);
    assert(allocated <= need);
    need -= allocated;
    if (need == 0) {
        return size;
    }

    allocated = AllocFromBitmapExtent(need, hint, exts);
    assert(allocated <= need);
    need -= allocated;
    if (need == 0) {
        return size;
    }

    allocated = AllocFromSmallExtent(need, hint, exts);
    assert(allocated <= need);

    need -= allocated;
    return size - need;
}

uint64_t BitmapAllocator::AllocFromBitmap(uint64_t size,
                                          const SpaceAllocateHint& hint,
                                          std::vector<PExtent>* exts) {
    uint64_t need = size;

    while (need > 0) {
        auto idx = bitmap_.NextClearBit(bitmapAllocIdx_);
        if (idx == bitmap_.NO_POS) {
            bitmapAllocIdx_ = 0;
            idx = bitmap_.NextClearBit(bitmapAllocIdx_);
            if (idx == bitmap_.NO_POS) {
                break;
            }
        }

        assert(idx != bitmap_.NO_POS);

        // mark this slot used
        bitmap_.Set(idx);
        bitmapAllocIdx_ = idx + 1;

        uint64_t off = idx * opt_.sizePerBit + bitmapAreaOffset_;
        assert(off >= bitmapAreaOffset_ &&
               off < opt_.startOffset + opt_.length);

        if (need >= opt_.sizePerBit) {
            exts->emplace_back(off, opt_.sizePerBit);
            need -= opt_.sizePerBit;
        } else {
            exts->emplace_back(off, need);
            bitmapExtent_.DeAlloc(off + need, opt_.sizePerBit - need);
            need = 0;
        }
    }

    return size - need;
}

uint64_t BitmapAllocator::AllocFromBitmapExtent(uint64_t size,
                                                const SpaceAllocateHint& hint,
                                                std::vector<PExtent>* exts) {
    return bitmapExtent_.Alloc(size, hint, exts);
}

uint64_t BitmapAllocator::AllocFromSmallExtent(uint64_t size,
                                               const SpaceAllocateHint& hint,
                                               std::vector<PExtent>* exts) {
    return smallExtent_.Alloc(size, hint, exts);
}

void BitmapAllocator::DeAllocToSmallExtent(uint64_t off, uint64_t len) {
    smallExtent_.DeAlloc(off, len);
}

void BitmapAllocator::DeAllocToBitmap(uint64_t off, uint64_t len) {
    uint64_t alignedLeftOff = p2roundup<uint64_t>(off, opt_.sizePerBit);
    uint64_t unalignedLeftLen = alignedLeftOff - off;

    uint64_t alignedRightOff = p2align<uint64_t>(off + len, opt_.sizePerBit);
    uint64_t unalignedRightLen = off + len - alignedRightOff;

    // DeAllocToBitmap
    if (alignedRightOff > alignedLeftOff) {
        auto curOff = alignedLeftOff;
        while (curOff < alignedRightOff) {
            auto idx = (curOff - bitmapAreaOffset_) / opt_.sizePerBit;
            bitmap_.Clear(idx);

            curOff += opt_.sizePerBit;
        }

        if (unalignedLeftLen != 0) {
            DeAllocToBitmapExtent(off, unalignedLeftLen);
        }
        if (unalignedRightLen != 0) {
            DeAllocToBitmapExtent(alignedRightOff, unalignedRightLen);
        }
    } else {
        DeAllocToBitmapExtent(off, len);
    }
}

void BitmapAllocator::DeAllocToBitmapExtent(uint64_t off, uint64_t len) {
    ExtentMapT blocks;
    bitmapExtent_.DeAlloc(off, len);
    auto size = bitmapExtent_.AvailableBlocks(&blocks);
    (void)size;

    for (const auto& e : blocks) {
        auto idx = (e.first - bitmapAreaOffset_) / opt_.sizePerBit;
        bitmap_.Clear(idx);
    }
}

void BitmapAllocator::MarkUsedInternal(uint64_t off, uint64_t len) {
    std::lock_guard<bthread::Mutex> lock(mtx_);

    uint64_t offInSmallExtent = 0;
    uint64_t lenInSmallExtent = 0;
    uint64_t offInBitmap = 0;
    uint64_t lenInBitmap = 0;

    Split(off, len, &offInSmallExtent, &lenInSmallExtent, &offInBitmap,
          &lenInBitmap);

    if (lenInSmallExtent != 0) {
        MarkUsedForSmallExtent(offInSmallExtent, lenInSmallExtent);
    }

    if (lenInBitmap != 0) {
        MarkUsedForBitmap(offInBitmap, lenInBitmap);
    }

    assert(available_ >= len);
    available_ -= len;
}

void BitmapAllocator::MarkUsedForSmallExtent(uint64_t off, uint64_t len) {
    smallExtent_.MarkUsed(off, len);
}

void BitmapAllocator::MarkUsedForBitmap(uint64_t off, uint64_t len) {
    // if it's a aligned block, mark slot used
    // otherwise call bitmapExtent::MarkUsed

    uint64_t alignedLeftOff = p2roundup<uint64_t>(off, opt_.sizePerBit);
    uint64_t unalignedLeftLen = alignedLeftOff - off;

    uint64_t alignedRightOff = p2align<uint64_t>(off + len, opt_.sizePerBit);
    uint64_t unalignedRightLen = off + len - alignedRightOff;

    // bitmap
    if (alignedRightOff > alignedLeftOff) {
        auto curOff = alignedLeftOff;
        while (curOff < alignedRightOff) {
            auto idx = (curOff - bitmapAreaOffset_) / opt_.sizePerBit;
            bitmap_.Set(idx);

            curOff += opt_.sizePerBit;
        }

        if (unalignedLeftLen != 0) {
            bitmapExtent_.MarkUsed(off, unalignedLeftLen);
        }
        if (unalignedRightLen != 0) {
            bitmapExtent_.MarkUsed(alignedRightOff, unalignedRightLen);
        }
    } else {
        bitmapExtent_.MarkUsed(off, len);
    }
}

void BitmapAllocator::Split(uint64_t off, uint64_t len,
                            uint64_t* offInSmallExtent,
                            uint64_t* lenInSmallExtent, uint64_t* offInBitmap,
                            uint64_t* lenInBitmap) const {
    if (off >= bitmapAreaOffset_) {
        *lenInSmallExtent = 0;
        *offInBitmap = off;
        *lenInBitmap = len;
        return;
    }

    if (off < bitmapAreaOffset_ && (off + len) <= bitmapAreaOffset_) {
        *lenInBitmap = 0;
        *offInSmallExtent = off;
        *lenInSmallExtent = len;
        return;
    }

    *offInSmallExtent = off;
    *lenInSmallExtent = bitmapAreaOffset_ - off;

    *offInBitmap = bitmapAreaOffset_;
    *lenInBitmap = off + len - bitmapAreaOffset_;
}

std::ostream& operator<<(std::ostream& os, const BitmapAllocator& alloc) {
    os << "avail: " << alloc.available_
       << ", [== bitmap ext: " << alloc.bitmapExtent_ << "  ==]"
       << ", [== small ext: " << alloc.smallExtent_ << " ==]";

    return os;
}

uint64_t BitmapAllocator::CalcBitmapAreaLength(
    const BitmapAllocatorOption& opt) {
    uint64_t len = p2align<uint64_t>(
        opt.length * (1 - opt.smallAllocProportion), opt.sizePerBit);

    return std::min(len, opt.length);
}

}  // namespace space
}  // namespace curvefs

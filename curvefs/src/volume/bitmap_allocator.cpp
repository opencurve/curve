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

#include "curvefs/src/volume/bitmap_allocator.h"

#include <glog/logging.h>

#include <algorithm>
#include <vector>

#include "curvefs/src/volume/common.h"
#include "curvefs/src/volume/utils.h"
#include "src/common/fast_align.h"

namespace curvefs {
namespace volume {

using curve::common::align_down;
using curve::common::align_up;
using curve::common::is_aligned;

namespace {

inline bool IsAllocTypeBig(const AllocateHint& hint) {
    return hint.allocType == AllocateType::Big;
}

constexpr uint64_t kAlignment = 4096;

}  // namespace

uint64_t BitmapAllocator::CalcBitmapAreaLength(
    const BitmapAllocatorOption& opt) {
    uint64_t len = align_down<uint64_t>(
        opt.length * (1 - opt.smallAllocProportion), opt.sizePerBit);

    return std::min(len, opt.length);
}

struct BitmapAllocator::AllocOrder {
    using Func = uint64_t (BitmapAllocator::*)(const uint64_t size,
                                               const AllocateHint& hint,
                                               std::vector<Extent>* exts);

    AllocOrder(Func first, Func second, Func third)
        : funs({first, second, third}) {}

    const std::vector<Func> funs;
};

BitmapAllocator::BitmapAllocator(const BitmapAllocatorOption& opt)
    : opt_(opt),
      bitmapAreaLength_(CalcBitmapAreaLength(opt)),
      smallAreaLength_(opt_.length - bitmapAreaLength_),
      bitmapAreaOffset_(opt_.startOffset + smallAreaLength_),
      mtx_(),
      available_(opt_.length),
      bitmapAllocIdx_(0),
      bitmap_(bitmapAreaLength_ / opt_.sizePerBit),
      bitmapExtent_(opt_.sizePerBit, bitmapAreaOffset_, bitmapAreaLength_),
      smallExtent_(opt_.startOffset, smallAreaLength_) {
    CHECK(bitmap_.Size() * opt_.sizePerBit + smallExtent_.AvailableSize() ==
          opt_.length)
        << "bitmap.size: " << bitmap_.Size()
        << ", opt.sizePerBit: " << opt_.sizePerBit
        << ", smallExtent.avail: " << smallExtent_.AvailableSize()
        << ", opt.length: " << opt.length;

    VLOG(9) << "offset: " << opt_.startOffset << ", len: " << opt_.length
            << ", size_per_bit: " << opt_.sizePerBit << "bitmapAreaLength_ "
            << bitmapAreaLength_ << ", bitmapAreaOffset_: " << bitmapAreaOffset_
            << ", smallAreaLength_: " << smallAreaLength_
            << ", available: " << available_;
}

BitmapAllocator::~BitmapAllocator() {}

uint64_t BitmapAllocator::Alloc(uint64_t size,
                                const AllocateHint& hint,
                                std::vector<Extent>* exts) {
    assert(is_aligned(size, kAlignment));

    static const BitmapAllocator::AllocOrder kAllocBig(
        &BitmapAllocator::AllocFromBitmap,
        &BitmapAllocator::AllocFromBitmapExtent,
        &BitmapAllocator::AllocFromSmallExtent);

    static const BitmapAllocator::AllocOrder kAllocBigWithHint(
        &BitmapAllocator::AllocFromBitmapExtent,
        &BitmapAllocator::AllocFromBitmap,
        &BitmapAllocator::AllocFromSmallExtent);

    static const BitmapAllocator::AllocOrder kAllocSmall(
        &BitmapAllocator::AllocFromSmallExtent,
        &BitmapAllocator::AllocFromBitmapExtent,
        &BitmapAllocator::AllocFromBitmap);

    std::lock_guard<bthread::Mutex> lock(mtx_);
    if (available_ == 0) {
        return 0;
    }

    uint64_t alloc = 0;

    if (size < opt_.sizePerBit && !IsAllocTypeBig(hint)) {
        alloc = AllocInternal(kAllocSmall, size, hint, exts);
    } else if (hint.HasHint()) {
        alloc = AllocInternal(kAllocBigWithHint, size, hint, exts);
    } else {
        alloc = AllocInternal(kAllocBig, size, hint, exts);
    }

    CHECK(alloc <= available_) << *this;
    available_ -= alloc;

    return alloc;
}

bool BitmapAllocator::DeAlloc(const uint64_t off, const uint64_t len) {
    assert(is_aligned(off, kAlignment) && is_aligned(len, kAlignment));

    assert(off >= opt_.startOffset &&
           off + len <= opt_.startOffset + opt_.length);

    VLOG(9) << "Dealloc off: " << off << ", len: " << len;

    std::lock_guard<bthread::Mutex> lock(mtx_);

    if (len > opt_.length - available_) {
        return false;
    }

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

    CHECK(available_ + len <= opt_.length) << *this;
    available_ += len;

    return true;
}

bool BitmapAllocator::DeAlloc(const std::vector<Extent>& exts) {
    for (const auto& e : exts) {
        if (!DeAlloc(e.offset, e.len)) {
            return false;
        }
    }

    return true;
}

bool BitmapAllocator::MarkUsed(const std::vector<Extent>& extents) {
    for (auto& e : extents) {
        MarkUsedInternal(e.offset, e.len);
    }

    return true;
}

bool BitmapAllocator::MarkUsable(const std::vector<Extent>& /*extents*/) {
    return false;
}

uint64_t BitmapAllocator::AllocInternal(const AllocOrder& order,
                                        const uint64_t size,
                                        const AllocateHint& hint,
                                        std::vector<Extent>* exts) {
    uint64_t need = size;
    for (auto& fn : order.funs) {
        need -= (this->*fn)(need, hint, exts);
        if (need == 0) {
            return size;
        }
    }

    return size - need;
}

uint64_t BitmapAllocator::AllocFromBitmap(uint64_t size,
                                          const AllocateHint& /*hint*/,
                                          std::vector<Extent>* exts) {
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

        // mark this slot used
        bitmap_.Set(idx);
        bitmapAllocIdx_ = idx + 1;

        uint64_t off = idx * opt_.sizePerBit + bitmapAreaOffset_;

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
                                                const AllocateHint& hint,
                                                std::vector<Extent>* exts) {
    return bitmapExtent_.Alloc(size, hint, exts);
}

uint64_t BitmapAllocator::AllocFromSmallExtent(uint64_t size,
                                               const AllocateHint& hint,
                                               std::vector<Extent>* exts) {
    return smallExtent_.Alloc(size, hint, exts);
}

void BitmapAllocator::DeAllocToSmallExtent(const uint64_t off,
                                           const uint64_t len) {
    smallExtent_.DeAlloc(off, len);
}

void BitmapAllocator::DeAllocToBitmap(const uint64_t off, const uint64_t len) {
    uint64_t alignedLeftOff = align_up<uint64_t>(off, opt_.sizePerBit);
    uint64_t unalignedLeftLen = alignedLeftOff - off;

    uint64_t alignedRightOff = align_down<uint64_t>(off + len, opt_.sizePerBit);
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

void BitmapAllocator::DeAllocToBitmapExtent(const uint64_t off,
                                            const uint64_t len) {
    std::map<uint64_t, uint64_t> blocks;
    bitmapExtent_.DeAlloc(off, len);
    auto size = bitmapExtent_.AvailableBlocks(&blocks);
    (void)size;

    for (const auto& e : blocks) {
        auto idx = (e.first - bitmapAreaOffset_) / opt_.sizePerBit;
        bitmap_.Clear(idx);
    }
}

void BitmapAllocator::MarkUsedInternal(const uint64_t off, const uint64_t len) {
    assert(is_aligned(off, kAlignment) && is_aligned(len, kAlignment));

    assert(off >= opt_.startOffset &&
           off + len <= opt_.startOffset + opt_.length);

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

    CHECK(available_ >= len) << *this;
    available_ -= len;
}

void BitmapAllocator::MarkUsedForSmallExtent(const uint64_t off,
                                             const uint64_t len) {
    smallExtent_.MarkUsed(off, len);
}

uint32_t BitmapAllocator::ToBitmapIndex(uint64_t offset) const {
    return (offset - bitmapAreaOffset_) / opt_.sizePerBit;
}

uint64_t BitmapAllocator::ToBitmapOffset(uint32_t index) const {
    return index * opt_.sizePerBit + bitmapAreaOffset_;
}

void BitmapAllocator::MarkUsedForBitmap(const uint64_t off,
                                        const uint64_t len) {
    // if it's a aligned block, mark slot used
    // otherwise call bitmapExtent::MarkUsed

    uint64_t alignedLeftOff = align_up<uint64_t>(off, opt_.sizePerBit);
    uint64_t unalignedLeftLen = alignedLeftOff - off;

    uint64_t alignedRightOff = align_down<uint64_t>(off + len, opt_.sizePerBit);
    uint64_t unalignedRightLen = off + len - alignedRightOff;

    // bitmap
    if (alignedRightOff > alignedLeftOff) {
        auto curOff = alignedLeftOff;
        while (curOff < alignedRightOff) {
            auto idx = ToBitmapIndex(curOff);
            assert(bitmap_.Test(idx) == false);
            bitmap_.Set(idx);
            curOff += opt_.sizePerBit;
        }

        if (unalignedLeftLen != 0) {
            auto idx = ToBitmapIndex(off);
            if (bitmap_.Test(idx)) {
                bitmapExtent_.MarkUsed(off, unalignedLeftLen);
            } else {
                bitmap_.Set(idx);
                bitmapExtent_.DeAlloc(ToBitmapOffset(idx),
                                      opt_.sizePerBit - unalignedLeftLen);
            }
        }
        if (unalignedRightLen != 0) {
            auto idx = ToBitmapIndex(off + len);
            if (bitmap_.Test(idx)) {
                bitmapExtent_.MarkUsed(alignedRightOff, unalignedRightLen);
            } else {
                bitmap_.Set(idx);
                bitmapExtent_.DeAlloc(off + len,
                                      opt_.sizePerBit - unalignedRightLen);
            }
        }
    } else {
        auto idx = ToBitmapIndex(off);
        if (bitmap_.Test(idx)) {
            bitmapExtent_.MarkUsed(off, len);
        } else {
            bitmap_.Set(idx);
            const auto startOff = ToBitmapOffset(idx);

            if (off != startOff) {
                bitmapExtent_.DeAlloc(startOff, off - startOff);
            }

            if ((off + len) != ToBitmapOffset(idx + 1)) {
                bitmapExtent_.DeAlloc(off + len,
                                    ToBitmapOffset(idx + 1) - (off + len));
            }
        }
    }
}

void BitmapAllocator::Split(const uint64_t off,
                            const uint64_t len,
                            uint64_t* offInSmallExtent,
                            uint64_t* lenInSmallExtent,
                            uint64_t* offInBitmap,
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

}  // namespace volume
}  // namespace curvefs

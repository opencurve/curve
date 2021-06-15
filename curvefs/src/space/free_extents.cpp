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

#include "curvefs/src/space/free_extents.h"

#include <glog/logging.h>

#include <iostream>
#include <ostream>
#include <sstream>
#include <utility>

#include "curvefs/src/space/common.h"
#include "curvefs/src/space/fast_align.h"
#include "curvefs/src/space/utils.h"

namespace curvefs {
namespace space {

FreeExtents::FreeExtents(const uint64_t off, const uint64_t len)
    : maxLength_(len),
      maxExtentSize_(0),
      available_(len),
      extents_(),
      blocks_() {
    if (len != 0) {
        extents_.emplace(off, maxLength_);
    }
}

FreeExtents::FreeExtents(const uint64_t maxExtentSize)
    : maxLength_(0),
      maxExtentSize_(maxExtentSize),
      available_(0),
      extents_(),
      blocks_() {}

uint64_t FreeExtents::AllocInternal(const uint64_t size,
                                    const SpaceAllocateHint& hint,
                                    Extents* exts) {
    if (available_ == 0) {
        return 0;
    }

    uint64_t need = size;
    ExtentMap::const_iterator iter;

    // 1. find extents that satisfy hint.leftOffset
    if (hint.leftOffset != SpaceAllocateHint::INVALID_OFFSET) {
        iter = extents_.lower_bound(hint.leftOffset);

        if (iter != extents_.end() && iter->first == hint.leftOffset) {
            if (iter->second >= need) {
                exts->emplace_back(iter->first, need);

                if (iter->second == need) {
                    extents_.erase(iter);
                } else {
                    auto newOff = iter->first + need;
                    auto newLen = iter->second - need;
                    extents_.erase(iter);
                    extents_.emplace(newOff, newLen);
                }

                return size;
            } else {
                need -= iter->second;
                exts->emplace_back(iter->first, iter->second);

                extents_.erase(iter);
            }
        }
    }

    // 2. find extents that satisfy hint.rightOffset
    // TODO(wuhanqing): fix bug in here
    // case hint.rightOffset = 2 * kMiB, need = 4 * MiB
    // second condition will overflow
    if (hint.rightOffset != SpaceAllocateHint::INVALID_OFFSET &&
        hint.rightOffset - need >= 0) {
        iter = extents_.lower_bound(hint.rightOffset - need);
        if (iter != extents_.end() &&
            iter->first == (hint.rightOffset - need)) {
            if (iter->second >= need) {
                exts->emplace_back(iter->first, need);

                if (iter->second == need) {
                    extents_.erase(iter);
                } else {
                    auto newOff = iter->first + need;
                    auto newLen = iter->second - need;
                    extents_.erase(iter);
                    extents_.emplace(newOff, newLen);
                }

                return size;
            } else {
                need -= iter->second;
                exts->emplace_back(iter->first, iter->second);

                extents_.erase(iter);
            }
        }
    }

    // both leftOffset and rightOffset aren't satisfied
    // first loop find a extent that satisfy needed size
    iter = extents_.begin();
    while (iter != extents_.end()) {
        if (iter->second >= need) {
            exts->emplace_back(iter->first, need);

            // modify this extents
            if (iter->second == need) {
                extents_.erase(iter);
            } else {
                auto newOff = iter->first + need;
                auto newLen = iter->second - need;
                extents_.erase(iter);
                extents_.emplace(newOff, newLen);
            }

            return size;
        } else {
            ++iter;
        }
    }

    // second loop consume all existing extent
    iter = extents_.begin();
    while (need > 0 && iter != extents_.end()) {
        if (iter->second <= need) {
            need -= iter->second;
            exts->emplace_back(iter->first, iter->second);

            // remove this extent
            iter = extents_.erase(iter);
        } else {  // iter->second > need
            exts->emplace_back(iter->first, need);

            auto newOff = iter->first + need;
            auto newLen = iter->second - need;

            extents_.erase(iter);
            extents_.emplace(newOff, newLen);

            return size;
        }
    }

    return size - need;
}

uint64_t FreeExtents::AvailableBlocks(ExtentMap* blocks) {
    uint64_t size = 0;

    // TODO(wuhanqing): move this calc to DeAlloc
    *blocks = std::move(blocks_);
    for (const auto& b : *blocks) {
        size += b.second;
    }

    available_ -= size;
    return size;
}

std::ostream& operator<<(std::ostream& os, const FreeExtents& e) {
    os << "avail: " << e.available_ << ", extents: ";

    for (auto& ee : e.extents_) {
        os << PExtent(ee.first, ee.second) << " ";
    }

    os << ", blocks: ";
    for (auto& ee : e.blocks_) {
        os << PExtent(ee.first, ee.second) << " ";
    }

    return os;
}

void FreeExtents::DeAllocInternal(const uint64_t off, const uint64_t len) {
    if (available_ == 0) {
        // FIXME: off/len may need recycle to blocks
        extents_.emplace(off, len);
        return;
    }

    // try merge with left extent
    auto iter = extents_.lower_bound(off);
    auto curIter = extents_.end();
    if (iter != extents_.begin()) {
        --iter;
    }
    if ((iter->first + iter->second) == off) {
        iter->second += len;
        curIter = iter;
    } else {
        // TODO(wuhanqing): should tackle iter->first + iter->second > off ?
        auto r = extents_.emplace(off, len);
        curIter = r.first;
    }

    // try merge with right extent
    const auto endOff = curIter->first + curIter->second;
    iter = extents_.lower_bound(endOff);
    if (iter != extents_.end() && iter->first == endOff) {
        curIter->second += iter->second;

        // erase current iterator
        extents_.erase(iter);
    }

    // split it if it's big enough
    if (maxExtentSize_ != 0 && curIter->second >= maxExtentSize_) {
        if (curIter->second == maxExtentSize_) {
            // not aligned to maxExtentSize
            if (align_down(curIter->first, maxExtentSize_) != curIter->first) {
                return;
            }

            blocks_.emplace(curIter->first, curIter->second);
            extents_.erase(curIter);
            return;
        } else {
            auto start = curIter->first;
            auto end = start + curIter->second;
            extents_.erase(curIter);

            auto alignStart = align_up(start, maxExtentSize_);
            auto alignEnd = align_down(end, maxExtentSize_);

            if (start != alignStart) {
                extents_.emplace(start, alignStart - start);
            }
            if (end != alignEnd) {
                extents_.emplace(alignEnd, end - alignEnd);
            }

            while (alignStart < alignEnd) {
                blocks_.emplace(alignStart, maxExtentSize_);
                alignStart += maxExtentSize_;
            }
        }
    }
}

void FreeExtents::MarkUsedInternal(const uint64_t off, const uint64_t len) {
    CHECK(available_ >= len) << "MarkUsedInternal: [off: " << off
                             << " ~ len: " << len << "] " << *this;

    auto iter = extents_.lower_bound(off);
    if (iter != extents_.end() && iter->first == off) {
        if (iter->first == off && iter->second == len) {
            extents_.erase(iter);
        } else {
            auto newOff = iter->first + len;
            auto newLen = iter->second - len;
            extents_.erase(iter);
            extents_.emplace(newOff, newLen);
        }
    } else {
        --iter;
        if ((iter->first + iter->second) == (off + len)) {
            auto newOff = iter->first;
            auto newLen = iter->second - len;
            extents_.erase(iter);
            extents_.emplace(newOff, newLen);
        } else {
            // [off, len] is in the middle of [iter->first, iter->second]
            auto leftOff = iter->first;
            auto leftLen = off - iter->first;
            auto rightOff = off + len;
            auto rightLen = (iter->first + iter->second) - rightOff;
            extents_.erase(iter);
            extents_.emplace(leftOff, leftLen);
            extents_.emplace(rightOff, rightLen);
        }
    }
}

}  // namespace space
}  // namespace curvefs

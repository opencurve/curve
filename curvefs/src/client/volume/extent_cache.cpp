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
 * Date: Monday Mar 14 19:42:09 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/client/volume/extent_cache.h"

#include <butil/time.h>
#include <bvar/bvar.h>
#include <glog/logging.h>

#include <algorithm>
#include <ostream>
#include <utility>

#include "curvefs/src/client/volume/extent_slice.h"
#include "curvefs/src/client/volume/metric.h"
#include "curvefs/src/common/metric_utils.h"
#include "include/client/libcurve.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/fast_align.h"

namespace curvefs {
namespace client {

using ::curve::common::align_down;
using ::curve::common::align_up;
using ::curve::common::is_aligned;
using ::curve::common::is_alignment;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;
using ::curvefs::common::LatencyUpdater;
using ::curvefs::metaserver::VolumeExtent;

namespace {

bvar::LatencyRecorder g_write_divide_latency("extent_cache_write_divide");
bvar::LatencyRecorder g_read_divide_latency("extent_cache_read_divide");
bvar::LatencyRecorder g_merge_latency("extent_cache_merge");
bvar::LatencyRecorder g_mark_written_latency("extent_cache_mark_written");

std::ostream& operator<<(std::ostream& os, const ExtentCacheOption& opt) {
    os << "prealloc size: " << opt.preAllocSize
       << ", slice size: " << opt.sliceSize
       << ", block size: " << opt.blockSize;

    return os;
}

}  // namespace

ExtentCacheOption ExtentCache::option_;

// TODO(wuhanqing): loffset and pext can span on two ranges
void ExtentCache::Merge(uint64_t loffset, const PExtent& pExt) {
    VLOG(9) << "merge extent, loffset: " << loffset
            << ", physical offset: " << pExt.pOffset << ", len: " << pExt.len
            << ", written: " << !pExt.UnWritten;
    assert(is_aligned(loffset, option_.blockSize));
    assert(is_aligned(pExt.pOffset, option_.blockSize));
    assert(is_aligned(pExt.len, option_.blockSize));

    LatencyUpdater updater(&g_merge_latency);
    WriteLockGuard lk(lock_);

    const auto sliceOffset = align_down(loffset, option_.sliceSize);
    auto slice = slices_.find(sliceOffset);
    if (slice == slices_.end()) {
        auto ret = slices_.emplace(sliceOffset, ExtentSlice{sliceOffset});
        assert(ret.second);
        slice = ret.first;
    }

    slice->second.Merge(loffset, pExt);
    dirties_.insert(&slice->second);
    VLOG(9) << "merge extent, loffset: " << loffset
            << ", physical offset: " << pExt.pOffset << ", len: " << pExt.len
            << ", written: " << !pExt.UnWritten
            << ", slice: " << slice->second.ToVolumeExtentSlice().DebugString();
}

void ExtentCache::DivideForWrite(uint64_t offset,
                                 uint64_t len,
                                 const char* data,
                                 std::vector<WritePart>* allocated,
                                 std::vector<AllocPart>* needAlloc) {
    LatencyUpdater updater(&g_write_divide_latency);
    ReadLockGuard lk(lock_);

    const auto end = offset + len;
    const char* datap = data;

    while (offset < end) {
        const auto length = std::min(
            end - offset, option_.sliceSize - (offset & ~option_.sliceSize));

        auto slice = slices_.find(align_down(offset, option_.sliceSize));
        if (slice != slices_.end()) {
            slice->second.DivideForWrite(offset, length, datap, allocated,
                                         needAlloc);
        } else {
            DivideForWriteWithinEmptySlice(offset, length, datap, needAlloc);
        }

        datap += length;
        offset += length;
    }
}

void ExtentCache::DivideForWriteWithinEmptySlice(
    uint64_t offset,
    uint64_t len,
    const char* data,
    std::vector<AllocPart>* needAlloc) {
    AllocPart part;
    part.data = data;

    auto alignedoffset = align_down(offset, option_.blockSize);
    part.allocInfo.lOffset = alignedoffset;

    uint64_t alloclength = 0;
    if (offset == alignedoffset) {
        alloclength =
            align_up(std::max(len, option_.preAllocSize), option_.blockSize);
    } else {
        auto alignedend = align_up(offset + len, option_.blockSize);
        alloclength =
            align_up(std::max(alignedend - alignedoffset, option_.preAllocSize),
                     option_.blockSize);
    }

    part.allocInfo.len = alloclength;

    const auto rangeEnd = align_up(offset + 1, option_.sliceSize);
    if (part.allocInfo.lOffset + part.allocInfo.len > rangeEnd) {
        part.allocInfo.len = rangeEnd - part.allocInfo.lOffset;
    }

    part.writelength = len;
    part.padding = offset - alignedoffset;

    needAlloc->push_back(part);
}

void ExtentCache::MarkWritten(uint64_t offset, uint64_t len) {
    LatencyUpdater updater(&g_mark_written_latency);
    WriteLockGuard lk(lock_);

    auto cur = align_down(offset, option_.blockSize);
    const auto end = align_up(offset + len, option_.blockSize);

    VLOG(9) << "mark written for offset: " << offset << ", len: " << len
            << ", cur: " << cur << ", end: " << end;

    while (cur < end) {
        const auto length =
            std::min(end - cur, option_.sliceSize - (cur & ~option_.sliceSize));
        auto slice = slices_.find(align_down(cur, option_.sliceSize));
        assert(slice != slices_.end());
        VLOG(9) << "mark written for offset: " << offset << ", len: " << len
                << ", cur: " << cur << ", end: " << end
                << ", before mark written slice: "
                << slice->second.ToVolumeExtentSlice().DebugString();
        auto changed = slice->second.MarkWritten(cur, length);
        VLOG(9) << "mark written for offset: " << offset << ", len: " << len
                << ", cur: " << cur << ", end: " << end
                << ", after mark written slice changed: " << changed
                << ", slice: "
                << slice->second.ToVolumeExtentSlice().DebugString();
        cur += length;
        if (changed) {
            dirties_.insert((&slice->second));
        }
    }
}

std::unordered_map<uint64_t, std::map<uint64_t, PExtent>>
ExtentCache::GetExtentsForTesting() const {
    std::unordered_map<uint64_t, std::map<uint64_t, PExtent>> result;
    ReadLockGuard lk(lock_);

    for (auto& slice : slices_) {
        result.emplace(slice.first, slice.second.GetExtentsForTesting());
    }

    return result;
}

void ExtentCache::DivideForRead(uint64_t offset,
                                uint64_t len,
                                char* data,
                                std::vector<ReadPart>* reads,
                                std::vector<ReadPart>* holes) {
    LatencyUpdater updater(&g_read_divide_latency);
    ReadLockGuard lk(lock_);
    VLOG(9) << "extent cache divide for read offset: " << offset
            << ", length: " << len;

    const auto end = offset + len;
    char* datap = data;

    while (offset < end) {
        const auto length = std::min(
            end - offset, option_.sliceSize - (offset & ~option_.sliceSize));

        auto slice = slices_.find(align_down(offset, option_.sliceSize));
        if (slice != slices_.end()) {
            slice->second.DivideForRead(offset, length, datap, reads, holes);
            VLOG(9) << "extent cache find slice for read offset: " << offset
                    << ", length: " << len << ", slice: "
                    << slice->second.ToVolumeExtentSlice().DebugString()
                    << ", slices size: " << slices_.size();
        } else {
            holes->emplace_back(offset, length, datap);
            VLOG(9) << "extent cache not find slice for read offset: " << offset
                    << ", length: " << len
                    << ", slices size: " << slices_.size();
        }

        datap += length;
        offset += length;
    }
}

void ExtentCache::SetOption(const ExtentCacheOption& option) {
    CHECK(is_alignment(option.preAllocSize))
        << "prealloc size must be power of 2, current is "
        << option.preAllocSize;
    CHECK(is_alignment(option.sliceSize))
        << "slice size must be power of 2, current is " << option.sliceSize;
    CHECK(is_alignment(option.blockSize))
        << "block size must be power of 2, current is " << option.blockSize;

    option_ = option;

    LOG(INFO) << "ExtentCacheOption: [" << option_ << "]";
}

void ExtentCache::Build(const VolumeExtentSliceList &extents) {
    WriteLockGuard lk(lock_);
    slices_.clear();
    dirties_.clear();

    for (const auto& s : extents.slices()) {
        slices_.emplace(s.offset(), ExtentSlice{s});
    }
}

VolumeExtentSliceList ExtentCache::GetDirtyExtents() {
    VolumeExtentSliceList result;
    WriteLockGuard lk(lock_);
    for (const auto* slice : dirties_) {
        *result.add_slices() = slice->ToVolumeExtentSlice();
    }

    dirties_.clear();
    VLOG(9) << "extent cache get and clear dirty extents";
    return result;
}

bool ExtentCache::HasDirtyExtents() const {
    curve::common::ReadLockGuard lk(lock_);
    return !dirties_.empty();
}

}  // namespace client
}  // namespace curvefs

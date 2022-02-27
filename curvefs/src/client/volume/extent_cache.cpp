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
#include <utility>

#include "absl/types/optional.h"
#include "curvefs/src/client/volume/metric.h"
#include "curvefs/src/common/metric_utils.h"
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
bvar::LatencyRecorder g_read_divide_latency_("extent_cache_read_divide");
bvar::LatencyRecorder g_merge_latency("extent_cache_merge");
bvar::LatencyRecorder g_mark_written_latency("extent_cache_mark_written");

}  // namespace

ExtentCacheOption ExtentCache::option_;

// TODO(wuhanqing): loffset and pext can span on two ranges
void ExtentCache::Merge(uint64_t loffset, const PExtent& pExt) {
    VLOG(9) << "merge extent, loffset: " << loffset
            << ", physical offset: " << pExt.pOffset << ", len: " << pExt.len
            << ", writtern: " << !pExt.UnWritten;
    assert(is_aligned(loffset, option_.blocksize));
    assert(is_aligned(pExt.pOffset, option_.blocksize));
    assert(is_aligned(pExt.len, option_.blocksize));

    LatencyUpdater updater(&g_merge_latency);
    WriteLockGuard lk(lock_);
    MergeWithinRange(&extents_[align_down(loffset, option_.rangeSize)], loffset,
                     pExt);
}

void ExtentCache::MergeWithinRange(std::map<uint64_t, PExtent>* range,
                                   uint64_t loffset,
                                   const PExtent& extent) {
    if (range->empty()) {
        range->emplace(loffset, extent);
        return;
    }

    // try merge with leftside
    auto it = range->lower_bound(loffset);
    auto inserted = range->end();

    if (it != range->begin()) {
        --it;
    }

    if (!it->second.UnWritten && it->first + it->second.len == loffset &&
        it->second.pOffset + it->second.len == extent.pOffset) {
        it->second.len += extent.len;
        inserted = it;
    } else {
        auto r = range->emplace(loffset, extent);
        inserted = r.first;
    }

    // try merge with rightside
    const auto endOff = inserted->first + inserted->second.len;
    it = range->lower_bound(endOff);

    if (!it->second.UnWritten && it != range->end() && it->first == endOff &&
        inserted->second.pOffset + inserted->second.len == it->second.pOffset) {
        inserted->second.len += it->second.len;
        range->erase(it);
    }
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
            end - offset, option_.rangeSize - (offset & ~option_.rangeSize));

        auto range = extents_.find(align_down(offset, option_.rangeSize));
        if (range != extents_.end()) {
            DivideForWriteWithinRange(range->second, offset, length, datap,
                                      allocated, needAlloc);
        } else {
            DivideForWriteWithinEmptyRange(offset, length, datap, needAlloc);
        }

        datap += length;
        offset += length;
    }
}

void ExtentCache::DivideForWriteWithinEmptyRange(
    uint64_t offset,
    uint64_t len,
    const char* data,
    std::vector<AllocPart>* needAlloc) {
    AllocPart part;
    part.data = data;

    auto alignedoffset = align_down(offset, option_.blocksize);
    part.allocInfo.lOffset = alignedoffset;

    uint64_t alloclength = 0;
    if (offset == alignedoffset) {
        alloclength =
            align_up(std::max(len, option_.preallocSize), option_.blocksize);
    } else {
        auto alignedend = align_up(offset + len, option_.blocksize);
        alloclength =
            align_up(std::max(alignedend - alignedoffset, option_.preallocSize),
                     option_.blocksize);
    }

    part.allocInfo.len = alloclength;

    const auto rangeEnd = align_up(offset + 1, option_.rangeSize);
    if (part.allocInfo.lOffset + part.allocInfo.len > rangeEnd) {
        part.allocInfo.len = rangeEnd - part.allocInfo.lOffset;
    }

    part.writelength = len;
    part.padding = offset - alignedoffset;

    needAlloc->push_back(part);
}

void ExtentCache::DivideForWriteWithinRange(
    const std::map<uint64_t, PExtent>& range,
    uint64_t offset,
    uint64_t len,
    const char* data,
    std::vector<WritePart>* allocated,
    std::vector<AllocPart>* needAlloc) {
    uint64_t curOff = offset;
    const char* datap = data;
    const uint64_t curEnd = offset + len;

    auto lower = range.lower_bound(curOff);
    const auto upper = range.upper_bound(curEnd);

    absl::optional<uint64_t> leftHintOffset;
    absl::optional<uint64_t> rightHintOffset;

    auto setAllocHint = [&leftHintOffset, &rightHintOffset](AllocPart* part) {
        if (leftHintOffset) {
            part->allocInfo.leftHintAvailable = true;
            part->allocInfo.pOffsetLeft = leftHintOffset.value();
        } else if (rightHintOffset) {
            part->allocInfo.rightHintAvailable = true;
            part->allocInfo.pOffsetRight = rightHintOffset.value();
        }
    };

    if (lower != range.begin()) {
        --lower;
    }

    while (curOff < curEnd && lower != upper) {
        const auto extStart = lower->first;
        const auto extEnd = lower->first + lower->second.len;

        if (curOff < extStart) {
            AllocPart part;
            part.data = datap;
            if (curEnd <= extStart) {
                // write    |----|           |----|
                // extent        |----|               |----|
                auto alignedoffset = align_down(curOff, option_.blocksize);
                auto alignedend = align_up(curEnd, option_.blocksize);

                assert(alignedend <= extStart);

                part.allocInfo.lOffset = alignedoffset;
                part.allocInfo.len = alignedend - alignedoffset;
                part.writelength = curEnd - curOff;
                part.padding = curOff - alignedoffset;

                // only set one side hint
                if (curEnd == extStart && !leftHintOffset) {
                    rightHintOffset = lower->second.pOffset;
                }

                ++lower;
            } else {
                // write   |-----|       |-------|    |--------|
                // extent      |----|       |----|      |----|
                auto alignedoffset = align_down(curOff, option_.blocksize);

                assert(is_aligned(extStart, option_.blocksize));

                part.allocInfo.lOffset = alignedoffset;
                part.allocInfo.len = extStart - alignedoffset;
                part.writelength = extStart - curOff;
                part.padding = curOff - alignedoffset;

                // only set one side hint
                if (!leftHintOffset) {
                    rightHintOffset = lower->second.pOffset;
                }
            }

            curOff += part.writelength;
            datap += part.writelength;
            setAllocHint(&part);
            needAlloc->push_back(part);
        } else if (curOff == extStart) {
            WritePart part;
            part.data = datap;

            // write   |----|   |----|      |-------|
            // extent  |----|   |--------|  |----|
            if (curEnd <= extEnd) {
                // write   |----|   |----|
                // extent  |----|   |--------|
                part.offset = lower->second.pOffset;
                part.length = (curEnd - curOff);
            } else {
                // write   |-------|
                // extent  |----|
                part.offset = lower->second.pOffset;
                part.length = (extEnd - extStart);
                leftHintOffset = lower->second.pOffset + lower->second.len;
                ++lower;
            }

            curOff += part.length;
            datap += part.length;
            allocated->push_back(part);
        } else {  // curOff > extStart
            WritePart part;
            part.data = datap;
            // write           |----|        |----|      |----|   |-----|        |----|   // NOLINT(whitespace/line_length)
            // extents  |----|          |----|         |------|  |-------|   |-----|      // NOLINT(whitespace/line_length)
            if (curOff >= extEnd) {
                // write           |----|        |----|
                // extents  |----|          |----|
                if (curOff == extEnd ||
                    align_down(curOff, option_.blocksize) == extEnd) {
                    leftHintOffset = lower->second.pOffset + lower->second.len;
                }

                ++lower;
                continue;
            } else if (curEnd <= extEnd) {
                // write       |----|    |----|
                // extents   |------|  |--------|
                part.offset = lower->second.pOffset + (curOff - extStart);
                part.length = curEnd - curOff;
            } else {  // curend > extExt
                // write        |----|
                // extents    |----|
                part.offset = lower->second.pOffset + (curOff - extStart);
                part.length = extEnd - curOff;
                leftHintOffset = lower->second.pOffset + lower->second.len;
                ++lower;
            }

            curOff += part.length;
            datap += part.length;
            allocated->push_back(part);
        }
    }

    if (curOff < curEnd) {
        AllocPart part;
        part.data = datap;

        auto alignedoffset = align_down(curOff, option_.blocksize);
        auto alignedend = align_up(curEnd, option_.blocksize);

        part.allocInfo.lOffset = alignedoffset;
        part.allocInfo.len =
            align_up(std::max(alignedend - alignedoffset, option_.preallocSize),
                     option_.blocksize);

        if (upper != range.end()) {
            part.allocInfo.len =
                std::min(upper->first - alignedoffset, part.allocInfo.len);
            assert(is_aligned(part.allocInfo.len, option_.blocksize));
        }

        part.writelength = curEnd - curOff;
        part.padding = curOff - alignedoffset;
        setAllocHint(&part);
        needAlloc->push_back(part);
    }
}

void ExtentCache::MarkWritten(uint64_t offset, uint64_t len) {
    LatencyUpdater updater(&g_mark_written_latency);
    WriteLockGuard lk(lock_);

    auto cur = align_down(offset, option_.blocksize);
    const auto end = align_up(offset + len, option_.blocksize);

    while (cur < end) {
        const auto length =
            std::min(end - cur, option_.rangeSize - (cur & ~option_.rangeSize));
        MarkWrittenWithinRange(&extents_[align_down(cur, option_.rangeSize)],
                               cur, length);

        cur += length;
    }
}

// TODO(wuhanqing): merge continuous extents
void ExtentCache::MarkWrittenWithinRange(std::map<uint64_t, PExtent>* range,
                                         uint64_t offset,
                                         uint64_t len) {
    uint64_t curOff = offset;
    const uint64_t curEnd = offset + len;

    auto lower = range->lower_bound(curOff);
    const auto upper = range->upper_bound(curEnd);
    auto prev = range->end();

    if (lower != range->begin()) {
        --lower;
    }

    auto nonoverlap = [](uint64_t off1, uint64_t len1, uint64_t off2,
                         uint64_t len2) {
        return off1 + len1 <= off2 || off2 + len2 <= off1;
    };

    // merge written extents
    auto mergeable =
        [&prev, &range](
            std::map<uint64_t, curvefs::client::PExtent>::iterator current) {
            return prev != range->end() &&
                   !prev->second.UnWritten &&
                   !current->second.UnWritten &&
                   // logical is continuous
                   prev->first + prev->second.len == current->first &&
                   // physical is continuous
                   prev->second.pOffset + prev->second.len ==
                       current->second.pOffset;
        };

    while (curOff < curEnd && lower != upper) {
        if (nonoverlap(curOff, curEnd - curOff, lower->first,
                       lower->second.len)) {
            prev = lower;
            ++lower;
            continue;
        }

        if (!lower->second.UnWritten) {
            if (mergeable(lower)) {
                prev->second.len += lower->second.len;
                lower = range->erase(lower);
            } else {
                prev = lower;
                ++lower;
            }

            continue;
        }

        const auto extStart = lower->first;
        const auto extEnd = lower->first + lower->second.len;

        if (curOff < extStart) {
            if (curEnd >= extEnd) {
                // write     |-------|    |--------|
                // extent       |----|      |----|
                lower->second.UnWritten = false;
                if (mergeable(lower)) {
                    prev->second.len += lower->second.len;
                    lower = range->erase(lower);
                } else {
                    prev = lower;
                    ++lower;
                }

                curOff = extEnd;
            } else {
                // write   |----|
                // extent      |----|
                uint64_t overlap = curEnd - extStart;
                PExtent sep;
                sep.pOffset = lower->second.pOffset + overlap;
                sep.len = lower->second.len - overlap;
                sep.UnWritten = true;

                lower->second.len = overlap;
                lower->second.UnWritten = false;

                range->emplace(extStart + overlap, sep);
                return;
            }
        } else if (curOff == extStart) {
            // write   |----|   |----|      |-------|
            // extent  |----|   |--------|  |----|
            if (extEnd <= curEnd) {
                // write   |----|   |-------|
                // extent  |----|   |----|
                lower->second.UnWritten = false;
                if (mergeable(lower)) {
                    prev->second.len += lower->second.len;
                    lower = range->erase(lower);
                } else {
                    prev = lower;
                    ++lower;
                }

                curOff = extEnd;
            } else {
                // write   |----|
                // extent  |--------|
                uint64_t overlap = curEnd - curOff;
                PExtent sep;
                sep.pOffset = lower->second.pOffset + overlap;
                sep.len = lower->second.len - overlap;
                sep.UnWritten = true;

                lower->second.len = overlap;
                lower->second.UnWritten = false;

                if (mergeable(lower)) {
                    prev->second.len += lower->second.len;
                    range->erase(lower);
                }

                range->emplace(curEnd, sep);
                return;
            }
        } else {
            // write       |----|    |----|       |----|
            // extents   |------|  |--------|   |----|
            if (curEnd == extEnd) {
                // write       |----|
                // extents   |------|
                uint64_t overlap = curEnd - curOff;
                PExtent sep;
                sep.pOffset = lower->second.pOffset + (curOff - extStart);
                sep.len = overlap;
                sep.UnWritten = false;

                lower->second.len -= overlap;

                range->emplace(curOff, sep);

                // 这里是不是可以直接跳出了
                return;
            } else if (curEnd < extEnd) {
                // write        |----|
                // extents    |--------|
                uint64_t overlap = curEnd - curOff;

                PExtent middle;
                middle.pOffset = lower->second.pOffset + (curOff - extStart);
                middle.len = overlap;
                middle.UnWritten = false;

                PExtent right;
                right.pOffset = middle.pOffset + middle.len;
                right.len = extEnd - curEnd;
                right.UnWritten = true;

                lower->second.len = curOff - extStart;

                range->emplace(curOff, middle);
                range->emplace(curEnd, right);

                return;
            } else {
                // write        |----|
                // extents    |----|

                uint64_t overlap = extEnd - curOff;
                PExtent sep;
                sep.pOffset = lower->second.pOffset + (curOff - extStart);
                sep.len = overlap;
                sep.UnWritten = false;

                lower->second.len -= overlap;

                range->emplace(curOff, sep);

                curOff = extEnd;
                prev = lower;
                ++lower;
            }
        }
    }
}

std::unordered_map<uint64_t, std::map<uint64_t, PExtent>>
ExtentCache::GetExtentsForTesting() const {
    WriteLockGuard lk(lock_);
    return extents_;
}

google::protobuf::Map<uint64_t, curvefs::metaserver::VolumeExtentList>
ExtentCache::ToInodePb() const {
    google::protobuf::Map<uint64_t, curvefs::metaserver::VolumeExtentList> res;

    auto mergeable =
        [](const VolumeExtent* prev,
           const std::pair<const uint64_t, curvefs::client::PExtent>& ext) {
            if (!prev) {
                return false;
            }

            return prev->isused() != ext.second.UnWritten &&
                   prev->fsoffset() + prev->length() == ext.first &&
                   prev->volumeoffset() + prev->length() == ext.second.pOffset;
        };

    ReadLockGuard lk(lock_);

    for (const auto& range : extents_) {
        curvefs::metaserver::VolumeExtentList& lis = res[range.first];
        auto* pbExt = lis.mutable_volumeextents();
        pbExt->Reserve(range.second.size());

        VolumeExtent* prev = nullptr;

        for (const auto& ext : range.second) {
            if (mergeable(prev, ext)) {
                prev->set_length(prev->length() + ext.second.len);
                continue;;
            }

            auto* t = pbExt->Add();
            t->set_fsoffset(ext.first);
            t->set_volumeoffset(ext.second.pOffset);
            t->set_length(ext.second.len);
            t->set_isused(!ext.second.UnWritten);
            prev = t;
        }
    }

    return res;
}

void ExtentCache::DivideForRead(uint64_t offset,
                                uint64_t len,
                                char* data,
                                std::vector<ReadPart>* reads,
                                std::vector<ReadPart>* holes) {
    LatencyUpdater updater(&g_read_divide_latency_);
    ReadLockGuard lk(lock_);

    const auto end = offset + len;
    char* datap = data;

    while (offset < end) {
        const auto length = std::min(
            end - offset, option_.rangeSize - (offset & ~option_.rangeSize));

        auto range = extents_.find(align_down(offset, option_.rangeSize));
        if (range != extents_.end()) {
            DivideForReadWithinRange(range->second, offset, length, datap,
                                     reads, holes);
        } else {
            holes->emplace_back(offset, length, datap);
        }

        datap += length;
        offset += length;
    }
}

void ExtentCache::DivideForReadWithinRange(
    const std::map<uint64_t, PExtent>& range,
    uint64_t offset,
    uint64_t len,
    char* data,
    std::vector<ReadPart>* reads,
    std::vector<ReadPart>* holes) {
    uint64_t curOff = offset;
    const uint64_t curEnd = offset + len;
    char* datap = data;

    auto lower = range.lower_bound(curOff);
    const auto upper = range.upper_bound(curEnd);

    if (lower != range.begin()) {
        --lower;
    }

    while (curOff < curEnd && lower != upper) {
        const auto extStart = lower->first;
        const auto extEnd = lower->first + lower->second.len;

        if (curOff < extStart) {
            if (curEnd <= extStart) {
                // read    |----|           |----|
                // extent       |----|               |----|
                holes->emplace_back(curOff, curEnd - curOff, datap);
                return;
            } else {
                // read    |----|        |-------|    |--------|
                // extent     |----|        |----|      |----|
                holes->emplace_back(curOff, extStart - curOff, datap);
            }

            datap += (extStart - curOff);
            curOff = extStart;
            // ++lower;
        } else if (curOff == extStart) {
            if (curEnd <= extEnd) {
                // read    |----|   |----|
                // extent  |----|   |--------|
                if (lower->second.UnWritten) {
                    holes->emplace_back(curOff, curEnd - curOff, datap);
                    return;
                } else {
                    reads->emplace_back(lower->second.pOffset, curEnd - curOff,
                                        datap);
                    return;
                }
            } else {
                // read    |-------|
                // extent  |----|
                if (lower->second.UnWritten) {
                    holes->emplace_back(curOff, extEnd - curOff, datap);
                } else {
                    reads->emplace_back(lower->second.pOffset, extEnd - curOff,
                                        datap);
                }

                datap += (extEnd - curOff);
                curOff = extEnd;
                ++lower;
            }
        } else {
            if (curOff >= extEnd) {
                // read            |----|        |----|
                // extents  |----|          |----|
                // do nothing, try next one
                ++lower;
                continue;
            } else if (curEnd <= extEnd) {
                // read        |----|    |----|
                // extents   |------|  |--------|
                if (lower->second.UnWritten) {
                    holes->emplace_back(curOff, curEnd - curOff, datap);
                } else {
                    reads->emplace_back(
                        lower->second.pOffset + (curOff - extStart),
                        curEnd - curOff, datap);
                }

                return;
            } else {
                // read         |----|
                // extents    |----|
                if (lower->second.UnWritten) {
                    holes->emplace_back(curOff, extEnd - curOff, datap);
                } else {
                    reads->emplace_back(
                        lower->second.pOffset + (curOff - extStart),
                        extEnd - curOff, datap);
                }

                datap += (extEnd - curOff);
                curOff = extEnd;
                ++lower;
            }
        }
    }

    if (curOff < curEnd) {
        holes->emplace_back(curOff, curEnd - curOff, datap);
    }
}

void ExtentCache::Build(
    const google::protobuf::Map<uint64_t,
                                curvefs::metaserver::VolumeExtentList>&
        fromInode) {
    WriteLockGuard lk(lock_);
    extents_.clear();

    for (const auto& l : fromInode) {
        auto& range = extents_[l.first];

        for (const auto& ext : l.second.volumeextents()) {
            range.emplace(
                ext.fsoffset(),
                PExtent{ext.length(), ext.volumeoffset(), !ext.isused()});
        }
    }
}

void ExtentCache::SetOption(const ExtentCacheOption& option) {
    CHECK(is_alignment(option_.preallocSize)) << option_.preallocSize;
    CHECK(is_alignment(option_.rangeSize)) << option_.rangeSize;
    option_ = option;
}

}  // namespace client
}  // namespace curvefs

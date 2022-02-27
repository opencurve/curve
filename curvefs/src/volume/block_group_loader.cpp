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
 * Date: Friday Mar 04 22:59:44 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/volume/block_group_loader.h"

#include <glog/logging.h>

#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "curvefs/proto/common.pb.h"
#include "curvefs/src/volume/allocator.h"
#include "curvefs/src/volume/block_device_client.h"
#include "curvefs/src/volume/block_group_updater.h"
#include "curvefs/src/volume/utils.h"
#include "src/common/bitmap.h"

namespace curvefs {
namespace volume {

using ::curve::common::BITMAP_UNIT_SIZE;
using ::curve::common::BitRange;

BitmapRange BlockGroupBitmapLoader::CalcBitmapRange() const {
    BitmapRange range;

    range.length = blockGroupSize_ / blockSize_ / BITMAP_UNIT_SIZE;

    switch (bitmapLocation_) {
        case BitmapLocation::AtStart: {
            range.offset = offset_;
            break;
        }

        case BitmapLocation::AtEnd: {
            range.offset = offset_ + blockGroupSize_ - range.length;
            break;
        }

        default:
            CHECK(false) << "not implemented";
    }

    return range;
}

std::ostream& operator<<(std::ostream& os, BitmapRange range) {
    os << "bitmap range offset: " << range.offset
       << ", length: " << range.length;

    return os;
}

bool BlockGroupBitmapLoader::Load(AllocatorAndBitmapUpdater* out) {
    BitmapRange bitmapRange = CalcBitmapRange();
    std::unique_ptr<char[]> data(new char[bitmapRange.length]);

    VLOG(9) << bitmapRange;

    auto err =
        blockDev_->Read(data.get(), bitmapRange.offset, bitmapRange.length);
    if (!err) {
        LOG(ERROR) << "Read bitmap from block device failed";
        return false;
    }

    AllocatorOption option = allocatorOption_;
    option.bitmapAllocatorOption.startOffset = offset_;
    option.bitmapAllocatorOption.length = blockGroupSize_;
    // TODO(wuhanqing): load from config
    option.bitmapAllocatorOption.sizePerBit = 4ull * 1024 * 1024;
    option.bitmapAllocatorOption.smallAllocProportion = 0;

    auto allocator = Allocator::Create(option.type, option);
    if (!allocator) {
        LOG(ERROR) << "Create allocator failed";
        return false;
    }

    Bitmap bitmap(bitmapRange.length * BITMAP_UNIT_SIZE, data.release());
    std::vector<BitRange> clearRange;
    std::vector<BitRange> setRange;
    bitmap.Divide(0, bitmapRange.length * BITMAP_UNIT_SIZE, &clearRange,
                  &setRange);

    // mark used
    // physical offset in volume
    std::vector<Extent> usedExtents;
    for (auto& range : setRange) {
        usedExtents.emplace_back(
            offset_ + range.beginIndex * blockSize_,
            (range.endIndex + 1 - range.endIndex) * blockSize_);
    }

    // bitmap location is also used
    usedExtents.emplace_back(bitmapRange.offset,   // physical offset
                             bitmapRange.length);  // physical length

    VLOG(9) << "markused, " << usedExtents
            << ", bitmap range offset: " << bitmapRange.offset
            << ", len: " << bitmapRange.length;

    if (!allocator->MarkUsed(usedExtents)) {
        LOG(ERROR) << "Init allocator from bitmap failed";
        return false;
    }

    out->allocator = std::move(allocator);
    out->bitmapUpdater = absl::make_unique<BlockGroupBitmapUpdater>(
        std::move(bitmap), blockSize_, blockGroupSize_, offset_, bitmapRange,
        blockDev_);

    return true;
}

}  // namespace volume
}  // namespace curvefs

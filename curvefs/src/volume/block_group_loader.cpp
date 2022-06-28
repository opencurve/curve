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
#include "src/common/fast_align.h"

namespace curvefs {
namespace volume {

using ::curve::common::BITMAP_UNIT_SIZE;
using ::curve::common::BitRange;
using ::curve::common::align_up;

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

    std::unique_ptr<Bitmap> bitmap;
    BitmapRange range;
    std::vector<Extent> used;
    if (!LoadBitmap(&bitmap, &range, &used)) {
        LOG(ERROR) << "Failed to load bitmap";
        return false;
    }

    if (!allocator->MarkUsed(used)) {
        LOG(ERROR) << "Failed to init allocator from bitmap";
        return false;
    }

    out->allocator = std::move(allocator);
    out->bitmapUpdater = absl::make_unique<BlockGroupBitmapUpdater>(
        std::move(*bitmap), blockSize_, blockGroupSize_, offset_, range,
        blockDev_);

    return true;
}

bool BlockGroupBitmapLoader::LoadBitmap(std::unique_ptr<Bitmap>* bitmap,
                                        BitmapRange* bitmapRange,
                                        std::vector<Extent>* used) {
    assert(bitmap != nullptr);
    assert(bitmapRange != nullptr);
    assert(used != nullptr);

    *bitmapRange = CalcBitmapRange();

    std::unique_ptr<char[]> data(new char[bitmapRange->length]);

    if (!clean_) {
        ssize_t err = blockDev_->Read(data.get(), bitmapRange->offset,
                                      bitmapRange->length);
        if (err != static_cast<ssize_t>(bitmapRange->length)) {
            LOG(ERROR) << "Failed to read bitmap from block device";
            return false;
        }

        *bitmap = absl::make_unique<Bitmap>(
            bitmapRange->length * BITMAP_UNIT_SIZE, data.release(),
            /*transfer*/ true);

        std::vector<BitRange> clearRange;
        std::vector<BitRange> setRange;
        (*bitmap)->Divide(0, bitmapRange->length * BITMAP_UNIT_SIZE,
                          &clearRange, &setRange);

        for (auto& range : setRange) {
            used->emplace_back(
                offset_ + range.beginIndex * blockSize_,
                (range.endIndex + 1 - range.beginIndex) * blockSize_);
        }
    } else {
        // we don't have mkfs now, so we clear the bitmap at first
        std::memset(data.get(), 0, bitmapRange->length);
        ssize_t err = blockDev_->Write(data.get(), bitmapRange->offset,
                                       bitmapRange->length);
        if (err != static_cast<ssize_t>(bitmapRange->length)) {
            LOG(ERROR) << "Failed to clear bitmap";
            return false;
        }

        *bitmap =
            absl::make_unique<Bitmap>(bitmapRange->length * BITMAP_UNIT_SIZE,
                                      data.release(), /*transfer*/ true);
    }

    used->emplace_back(bitmapRange->offset,  // physical offset
                       align_up<uint64_t>(bitmapRange->length, blockSize_));

    return true;
}

}  // namespace volume
}  // namespace curvefs

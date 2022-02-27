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
 * Date: Friday Mar 04 23:05:02 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/volume/block_group_updater.h"

#include <glog/logging.h>

#include "curvefs/src/volume/block_device_client.h"

namespace curvefs {
namespace volume {

void BlockGroupBitmapUpdater::Update(const Extent& ext, Op op) {
    std::lock_guard<std::mutex> lk(lock_);

    uint64_t startOffset = ext.offset - groupOffset_;
    uint64_t endOffset = ext.offset + ext.len - groupOffset_;
    uint64_t startIdx = startOffset / blockSize_;
    uint64_t endIdx = endOffset / blockSize_ - 1;

    if (op == Op::Set) {
        bitmap_.Set(startIdx, endIdx);
    } else {
        bitmap_.Clear(startIdx, endIdx);
    }

    dirty_ = true;
}

bool BlockGroupBitmapUpdater::Sync() {
    std::lock_guard<std::mutex> lk(lock_);

    if (!dirty_) {
        return true;
    }

    bool ret = blockDev_->Write(bitmap_.GetBitmap(), bitmapRange_.offset,
                                bitmapRange_.length);

    LOG_IF(ERROR, !ret) << "Sync block group bitmap failed, err: " << ret
                        << ", block group offset: " << groupOffset_;

    dirty_ = !ret;

    return ret;
}

}  // namespace volume
}  // namespace curvefs

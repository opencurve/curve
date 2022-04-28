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
 * Date: Friday Mar 04 23:04:55 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_VOLUME_BLOCK_GROUP_UPDATER_H_
#define CURVEFS_SRC_VOLUME_BLOCK_GROUP_UPDATER_H_

#include <mutex>
#include <utility>

#include "curvefs/src/volume/common.h"
#include "src/common/bitmap.h"

namespace curvefs {
namespace volume {

using ::curve::common::Bitmap;

class BlockDeviceClient;

struct BitmapRange {
    uint64_t offset;
    uint64_t length;
};

// bitmap updater for each block group
class BlockGroupBitmapUpdater {
 public:
    BlockGroupBitmapUpdater(Bitmap bitmap,
                            uint32_t blockSize,
                            uint32_t groupSize,
                            uint64_t groupOffset,
                            const BitmapRange& range,
                            BlockDeviceClient* blockDev)
        : dirty_(false),
          bitmap_(std::move(bitmap)),
          blockSize_(blockSize),
          groupSize_(groupSize),
          groupOffset_(groupOffset),
          bitmapRange_(range),
          blockDev_(blockDev) {}

    enum Op { Set, Clear };

    /**
     * @brief Update corresponding bit that covered by exts
     * @param op set or clear bit
     */
    void Update(const Extent& ext, Op op);

    /**
     * @brief Sync bitmap to backend storage if dirty
     * @return return true if success, otherwise, return false
     */
    bool Sync();

 private:
    std::mutex bitmapMtx_;
    std::mutex syncMtx_;
    bool dirty_;
    Bitmap bitmap_;
    uint32_t blockSize_;
    uint32_t groupSize_;
    uint64_t groupOffset_;
    BitmapRange bitmapRange_;
    BlockDeviceClient* blockDev_;
};

}  // namespace volume
}  // namespace curvefs

#endif  // CURVEFS_SRC_VOLUME_BLOCK_GROUP_UPDATER_H_

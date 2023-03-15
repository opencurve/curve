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

#ifndef CURVEFS_SRC_VOLUME_OPTION_H_
#define CURVEFS_SRC_VOLUME_OPTION_H_

#include <cstdint>
#include <string>

#include "curvefs/proto/common.pb.h"

namespace curvefs {
namespace volume {

struct BitmapAllocatorOption {
    uint64_t startOffset;
    uint64_t length;
    uint64_t sizePerBit;
    double smallAllocProportion;
};

struct BlockGroupManagerOption {
    uint32_t fsId;
    uint32_t blockGroupAllocateOnce;
    uint32_t blockSize;
    uint64_t blockGroupSize;
    std::string owner;
};

struct AllocatorOption {
    std::string type;
    BitmapAllocatorOption bitmapAllocatorOption;
};

struct SpaceManagerOption {
    AllocatorOption allocatorOption;
    BlockGroupManagerOption blockGroupManagerOption;

    double threshold{1.0};
    uint64_t releaseInterSec{300};
};

}  // namespace volume
}  // namespace curvefs

#endif  // CURVEFS_SRC_VOLUME_OPTION_H_

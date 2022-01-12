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

#ifndef CURVEFS_SRC_SPACE_CONFIG_H_
#define CURVEFS_SRC_SPACE_CONFIG_H_

#include <cstdint>
#include <string>

namespace curvefs {
namespace space {

struct BitmapAllocatorOption {
    uint64_t startOffset;
    uint64_t length;
    uint64_t sizePerBit;
    double smallAllocProportion;
};

struct AllocatorOption {
    BitmapAllocatorOption bitmapAllocatorOption;
};

struct MetaServerClientOption {
    std::string addr;
};

struct ReloaderOption {
    MetaServerClientOption metaServerOption;
};

struct SpaceManagerOption {
    uint32_t blockSize;
    std::string allocatorType;
    ReloaderOption reloaderOption;
    AllocatorOption allocatorOption;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_CONFIG_H_

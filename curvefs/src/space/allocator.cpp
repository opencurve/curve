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

#include "curvefs/src/space/allocator.h"

#include <memory>

#include "curvefs/src/space/bitmap_allocator.h"

namespace curvefs {
namespace space {

std::unique_ptr<Allocator> Allocator::Create(const std::string& type,
                                             const AllocatorOption& option) {
    if (type == "bitmap") {
        return std::unique_ptr<Allocator>(
            new BitmapAllocator(option.bitmapAllocatorOption));
    } else {
        return nullptr;
    }
}

}  // namespace space
}  // namespace curvefs

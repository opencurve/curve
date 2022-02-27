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

#include "curvefs/src/volume/allocator.h"

#include <gtest/gtest.h>

#include <type_traits>

#include "curvefs/src/volume/bitmap_allocator.h"
#include "curvefs/test/volume/common.h"

namespace curvefs {
namespace volume {

TEST(AllocatorTest, Common) {
    auto allocator = Allocator::Create("", {});
    EXPECT_FALSE(allocator);

    allocator = Allocator::Create("group", {});
    EXPECT_FALSE(allocator);

    AllocatorOption option;
    option.bitmapAllocatorOption.length = 10 * kGiB;
    option.bitmapAllocatorOption.sizePerBit = 4 * kMiB;
    option.bitmapAllocatorOption.smallAllocProportion = 0.2;
    allocator = Allocator::Create("bitmap", option);
    EXPECT_TRUE(allocator);

    auto* p = dynamic_cast<BitmapAllocator*>(allocator.get());
    EXPECT_TRUE(p);
}

}  // namespace volume
}  // namespace curvefs

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
 * Date: Thursday Mar 24 16:14:48 CST 2022
 * Author: wuhanqing
 */

#include <gtest/gtest.h>

#include "curvefs/src/client/common/config.h"

namespace curvefs {
namespace client {
namespace common {

using curve::common::Configuration;

void InitVolumeOption(Configuration *conf, VolumeOption *volumeOpt);

TEST(TestInitVolumeOption, Common) {
    Configuration conf;
    VolumeOption volopt;

    conf.SetUInt64Value("volume.bigFileSize", 1ULL * 1024 * 1024);
    conf.SetUInt64Value("volume.volBlockSize", 4096);
    conf.SetUInt64Value("volume.fsBlockSize", 4096);
    conf.SetUInt32Value("volume.blockGroup.allocateOnce", 4);
    conf.SetStringValue("volume.allocator.type", "bitmap");
    conf.SetUInt64Value("volume.bitmapAllocator.sizePerBit",
                        4ULL * 1024 * 1024);
    conf.SetDoubleValue("volume.bitmapAllocator.smallAllocProportion", 0.0);

    ASSERT_NO_FATAL_FAILURE({ InitVolumeOption(&conf, &volopt); });
}

TEST(TestInitVolumeOption, TypeError) {
    Configuration conf;
    VolumeOption volopt;

    conf.SetUInt64Value("volume.bigFileSize", 1ULL * 1024 * 1024);
    conf.SetUInt64Value("volume.volBlockSize", 4096);
    conf.SetUInt64Value("volume.fsBlockSize", 4096);
    conf.SetUInt32Value("volume.blockGroup.allocateOnce", 4);
    conf.SetStringValue("volume.allocator.type", "xxx");
    conf.SetUInt64Value("volume.bitmapAllocator.sizePerBit",
                        4ULL * 1024 * 1024);
    conf.SetDoubleValue("volume.bitmapAllocator.smallAllocProportion", 0.0);

    ASSERT_DEATH({ InitVolumeOption(&conf, &volopt); }, "");
}

}  // namespace common
}  // namespace client
}  // namespace curvefs

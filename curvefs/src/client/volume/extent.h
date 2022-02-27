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


/*
 * Project: curve
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_SRC_CLIENT_VOLUME_EXTENT_H_
#define CURVEFS_SRC_CLIENT_VOLUME_EXTENT_H_

#include <cstdint>

namespace curvefs {
namespace client {

struct ExtentAllocInfo {
    // logical offset within single inode/file, aligned to storage's block size
    uint64_t lOffset;
    uint64_t len;
    uint64_t pOffsetLeft;
    uint64_t pOffsetRight;
    bool leftHintAvailable = false;
    bool rightHintAvailable = false;
};

// physical extent
struct PExtent {
    uint64_t len = 0;
    uint64_t pOffset = 0;
    bool UnWritten = true;

    PExtent() = default;

    PExtent(uint64_t len, uint64_t poffset, bool unwritten)
        : len(len), pOffset(poffset), UnWritten(unwritten) {}
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VOLUME_EXTENT_H_

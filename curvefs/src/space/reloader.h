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

#ifndef CURVEFS_SRC_SPACE_RELOADER_H_
#define CURVEFS_SRC_SPACE_RELOADER_H_

#include <cstdint>

#include "curvefs/src/space/allocator.h"
#include "curvefs/src/space/config.h"

namespace curvefs {
namespace space {

class Reloader {
 public:
    Reloader(Allocator* allocator, uint32_t fsId, uint64_t rootInodeId,
             const ReloaderOption& option)
        : fsId_(fsId),
          rootInodeId_(rootInodeId),
          option_(option),
          allocator_(allocator) {}

    ~Reloader() = default;

    Reloader(const Reloader&) = delete;

    Reloader& operator=(const Reloader&) = delete;

    bool Reload();

 private:
    const uint32_t fsId_;
    const uint64_t rootInodeId_;

    ReloaderOption option_;

    Allocator* allocator_;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_RELOADER_H_

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

#ifndef CURVEFS_SRC_SPACE_ALLOCATOR_RELOADER_H_
#define CURVEFS_SRC_SPACE_ALLOCATOR_RELOADER_H_

#include <cstdint>

#include "curvefs/src/space_allocator/allocator.h"
#include "curvefs/src/space_allocator/s3_allocator.h"

namespace curvefs {
namespace space {

class Reloader {
 public:
    Reloader(uint32_t fsId, uint64_t rootInodeId)
        : fsId_(fsId), rootInodeId_(rootInodeId) {}

    virtual ~Reloader() = default;

    virtual bool Reload() = 0;

 protected:
    const uint32_t fsId_;
    const uint64_t rootInodeId_;

 private:
    Reloader(const Reloader&);
    Reloader& operator=(const Reloader&);
};

class BlockVolumeRealoder : public Reloader {
 public:
    BlockVolumeRealoder(Allocator* allocator, uint32_t fsId,
                        uint64_t rootInodeId)
        : Reloader(fsId, rootInodeId), allocator_(allocator) {}

    bool Reload() override;

 private:
    Allocator* allocator_;
};

class S3Reloader : public Reloader {
 public:
    S3Reloader(S3Allocator* allocator, uint32_t fsId, uint64_t rootInodeId)
        : Reloader(fsId, rootInodeId), allocator_(allocator) {}

    bool Reload() override;

 private:
    S3Allocator* allocator_;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_ALLOCATOR_RELOADER_H_

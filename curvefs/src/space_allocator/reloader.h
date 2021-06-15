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

#include "curvefs/src/space_allocator/allocator.h"

namespace curvefs {
namespace space {

class Reloader {
 public:
    explicit Reloader(Allocator* allocator) : allocator_(allocator) {}

    virtual ~Reloader() = default;

    virtual bool Reload() = 0;

 protected:
    Allocator* allocator_;

 private:
    Reloader(const Reloader&);
    Reloader& operator=(const Reloader&);
};

class DefaultRealoder : public Reloader {
 public:
    DefaultRealoder(Allocator* allocator, uint64_t fsId, uint64_t rootInodeId);

    ~DefaultRealoder();

    bool Reload() override;

 private:
    bool Reload(uint64_t inodeId);

 private:
    uint32_t fsId_;
    uint64_t rootInodeId_;
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_ALLOCATOR_RELOADER_H_

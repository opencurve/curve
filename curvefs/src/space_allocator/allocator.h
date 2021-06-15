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

#ifndef CURVEFS_SRC_SPACE_ALLOCATOR_ALLOCATOR_H_
#define CURVEFS_SRC_SPACE_ALLOCATOR_ALLOCATOR_H_

#include <cstdint>
#include <vector>

#include "curvefs/src/space_allocator/common.h"

namespace curvefs {
namespace space {

class Allocator {
 public:
    Allocator() = default;

    virtual ~Allocator() = default;

    virtual uint64_t Alloc(uint64_t size, const SpaceAllocateHint& hint,
                           std::vector<PExtent>* exts) = 0;

    virtual void DeAlloc(uint64_t off, uint64_t len) = 0;

    virtual void DeAlloc(const std::vector<PExtent>& exts) = 0;

    virtual uint64_t Total() const = 0;

    virtual uint64_t AvailableSize() const = 0;

    /**
     * @brief 标记对应空间已使用，初始化时使用
     */
    virtual bool MarkUsed(const std::vector<PExtent>& extents) = 0;

    /**
     * @brief 标记对应空间可以使用，初始化时使用
     */
    virtual bool MarkUsable(const std::vector<PExtent>& extents) = 0;

 private:
    Allocator(const Allocator&);
    Allocator& operator=(const Allocator&);
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_ALLOCATOR_ALLOCATOR_H_

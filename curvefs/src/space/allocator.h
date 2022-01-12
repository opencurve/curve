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

#ifndef CURVEFS_SRC_SPACE_ALLOCATOR_H_
#define CURVEFS_SRC_SPACE_ALLOCATOR_H_

#include <memory>
#include <string>

#include "curvefs/src/space/common.h"
#include "curvefs/src/space/config.h"

namespace curvefs {
namespace space {

class Allocator {
 public:
    Allocator() = default;

    virtual ~Allocator() = default;

    Allocator(const Allocator&) = delete;

    Allocator& operator=(const Allocator&) = delete;

    /**
     * @brief Allocate space
     *
     * @param size expected allocate space size
     * @param hint allocate hint of current allocation
     * @param[out] exts store allocated extents
     * @return return allocated size
     */
    virtual uint64_t Alloc(const uint64_t size, const SpaceAllocateHint& hint,
                           Extents* exts) = 0;

    /**
     * @brief DeAllocate space
     *
     * @return return true if succeeded, otherwise return false
     */
    virtual bool DeAlloc(const uint64_t off, const uint64_t len) = 0;

    /**
     * @brief DeAllocate space
     *
     * @return return true if succeeded, otherwise return false
     */
    virtual bool DeAlloc(const Extents& exts) = 0;

    /**
     * @brief Total space size
     */
    virtual uint64_t Total() const = 0;

    /**
     * @brief Current available space size
     */
    virtual uint64_t AvailableSize() const = 0;

    /**
     * @brief Mark extents are used
     */
    virtual bool MarkUsed(const Extents& extents) = 0;

    /**
     * @brief Mark extents are available
     */
    virtual bool MarkUsable(const Extents& extents) = 0;

    static std::unique_ptr<Allocator> Create(const std::string& type,
                                             const AllocatorOption& option);
};

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_ALLOCATOR_H_

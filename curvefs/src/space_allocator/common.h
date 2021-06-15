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

#ifndef CURVEFS_SRC_SPACE_ALLOCATOR_COMMON_H_
#define CURVEFS_SRC_SPACE_ALLOCATOR_COMMON_H_

#include <cstdint>
#include <limits>
#include <map>

#ifdef USE_BTREE
#include "absl/container/btree_map.h"
#error "There exists some cases that insert/delete an element into/from a btree_map may invalidate outstanding iterator"  // NOLINT
#endif  // USE_BTREE

#include "curvefs/proto/space.pb.h"

namespace curvefs {
namespace space {

#ifdef USE_BTREE
using ExtentMapT = absl::btree_map<uint64_t, uint64_t>;
#else
using ExtentMapT = std::map<uint64_t, uint64_t>;
#endif  // USE_BTREE

constexpr uint64_t kKiB = 1024ull;
constexpr uint64_t kMiB = 1024ull * kKiB;
constexpr uint64_t kGiB = 1024ull * kMiB;
constexpr uint64_t kTiB = 1024ull * kGiB;

struct SpaceAllocateHint {
    enum
    { INVALID_OFFSET = std::numeric_limits<uint64_t>::max() };

    AllocateType allocType = AllocateType::NONE;
    uint64_t leftOffset = INVALID_OFFSET;
    uint64_t rightOffset = INVALID_OFFSET;
};

struct PExtent {
    uint64_t offset;
    uint64_t len;

    PExtent() : PExtent(0, 0) {}
    PExtent(uint64_t o, uint64_t l) : offset(o), len(l) {}

    bool operator==(const PExtent& e) const {
        return offset == e.offset && len == e.len;
    }
};

template <typename T>
constexpr inline T p2align(T x, T align) {
    return x & -align;
}

template <typename T>
constexpr inline T p2roundup(T x, T align) {
    return -(-x & -align);
}

}  // namespace space
}  // namespace curvefs

#endif  // CURVEFS_SRC_SPACE_ALLOCATOR_COMMON_H_

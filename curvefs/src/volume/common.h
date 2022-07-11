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

#ifndef CURVEFS_SRC_VOLUME_COMMON_H_
#define CURVEFS_SRC_VOLUME_COMMON_H_

#include <unistd.h>

#include <cstdint>
#include <iosfwd>
#include <limits>
#include <vector>

namespace curvefs {
namespace volume {

constexpr uint64_t kKiB = 1024ULL;
constexpr uint64_t kMiB = 1024ULL * kKiB;
constexpr uint64_t kGiB = 1024ULL * kMiB;
constexpr uint64_t kTiB = 1024ULL * kGiB;

struct Extent {
    uint64_t offset = 0;
    uint64_t len = 0;

    Extent() = default;
    Extent(uint64_t o, uint64_t l) : offset(o), len(l) {}

    bool operator==(const Extent& e) const {
        return offset == e.offset && len == e.len;
    }
};

enum class AllocateType {
    None,
    Small,
    Big,
};

struct AllocateHint {
    enum { INVALID_OFFSET = std::numeric_limits<uint64_t>::max() };

    AllocateType allocType = AllocateType::None;
    uint64_t leftOffset = INVALID_OFFSET;
    uint64_t rightOffset = INVALID_OFFSET;

    bool HasLeftHint() const { return leftOffset != INVALID_OFFSET; }

    bool HasRightHint() const { return rightOffset != INVALID_OFFSET; }

    bool HasHint() const { return HasRightHint() || HasLeftHint(); }
};

struct SpaceStat {
    uint64_t total = 0;
    uint64_t available = 0;
    uint32_t blockSize = 0;

    SpaceStat() = default;
    SpaceStat(uint64_t total, uint64_t available, uint64_t blockSize)
        : total(total), available(available), blockSize(blockSize) {}
};

struct WritePart {
    off_t offset = 0;
    size_t length = 0;
    const char* data = nullptr;

    WritePart() = default;

    WritePart(off_t offset, size_t length, const char* data)
        : offset(offset), length(length), data(data) {}
};

struct ReadPart {
    off_t offset = 0;
    size_t length = 0;
    char* data = nullptr;

    ReadPart() = default;

    ReadPart(off_t offset, size_t length, char* data)
        : offset(offset), length(length), data(data) {}
};

std::ostream& operator<<(std::ostream& os, const Extent& e);

std::ostream& operator<<(std::ostream& os, const std::vector<Extent>& es);

std::ostream& operator<<(std::ostream& os, AllocateType type);

std::ostream& operator<<(std::ostream& os, const AllocateHint& hint);

}  // namespace volume
}  // namespace curvefs

#endif  // CURVEFS_SRC_VOLUME_COMMON_H_

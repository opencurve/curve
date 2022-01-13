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

#include "curvefs/src/space/utils.h"

#include <sstream>

namespace curvefs {
namespace space {

SpaceAllocateHint ConvertToSpaceAllocateHint(const AllocateHint& hint) {
    SpaceAllocateHint spaceHint;

    if (hint.has_alloctype()) {
        spaceHint.allocType = hint.alloctype();
    }

    if (hint.has_leftoffset()) {
        spaceHint.leftOffset = hint.leftoffset();
    }

    if (hint.has_rightoffset()) {
        spaceHint.rightOffset = hint.rightoffset();
    }

    return spaceHint;
}

// TODO(wuhanqing): sort and merge before convert to proto ?
ProtoExtents ConvertToProtoExtents(const Extents& exts) {
    ProtoExtents result;

    for (const auto& e : exts) {
        auto* p = result.Add();
        p->set_offset(e.offset);
        p->set_length(e.len);
    }

    return result;
}

std::ostream& operator<<(std::ostream& os, const Extent& e) {
    os << "[off: " << e.offset() << " ~ len: " << e.length() << "]";
    return os;
}

std::ostream& operator<<(std::ostream& os, const PExtent& e) {
    os << "[off: " << e.offset << " ~ len: " << e.len << "]";
    return os;
}

std::ostream& operator<<(std::ostream& os, const Extents& es) {
    std::ostringstream oss;

    for (const auto& e : es) {
        oss << e << " ";
    }

    os << oss.str();
    return os;
}

}  // namespace space
}  // namespace curvefs

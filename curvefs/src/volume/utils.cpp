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

#include "curvefs/src/volume/utils.h"

#include <sstream>

namespace curvefs {
namespace volume {

std::ostream& operator<<(std::ostream& os, const Extent& e) {
    os << "[off: " << e.offset << " ~ len: " << e.len << "]";
    return os;
}

std::ostream& operator<<(std::ostream& os, const std::vector<Extent>& es) {
    std::ostringstream oss;

    for (const auto& e : es) {
        oss << e << " ";
    }

    os << oss.str();
    return os;
}

std::ostream& operator<<(std::ostream& os, const AllocateHint& hint) {
    std::ostringstream oss;

    oss << "[type: " << hint.allocType;

    if (hint.HasLeftHint()) {
        oss << ", left hint: " << hint.leftOffset;
    }
    if (hint.HasRightHint()) {
        oss << ", right hint: " << hint.rightOffset;
    }

    oss << "]";

    os << oss.str();
    return os;
}

std::ostream& operator<<(std::ostream& os, AllocateType type) {
    switch (type) {
        case AllocateType::None:
            os << "none";
            break;
        case AllocateType::Big:
            os << "big";
            break;
        case AllocateType::Small:
            os << "small";
            break;
        default:
            os << "unknown";
            break;
    }

    return os;
}

}  // namespace volume
}  // namespace curvefs

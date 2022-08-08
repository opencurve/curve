/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Tuesday Jul 12 11:04:47 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/volume/common.h"

#include <ostream>
#include <sstream>
#include <string>

namespace curvefs {
namespace volume {

std::ostream& operator<<(std::ostream& os, const AllocateHint& hint) {
    auto offstr = [](uint64_t offset) {
        if (offset == AllocateHint::INVALID_OFFSET) {
            return std::string{"None"};
        }

        return std::to_string(offset);
    };

    os << "[type: " << hint.allocType << ", left: " << offstr(hint.leftOffset)
       << ", right: " << offstr(hint.rightOffset) << "]";

    return os;
}

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

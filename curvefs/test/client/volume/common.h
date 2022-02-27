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
 * Date: Tuesday Mar 15 20:06:07 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_CLIENT_VOLUME_COMMON_H_
#define CURVEFS_TEST_CLIENT_VOLUME_COMMON_H_

#include <cstdint>
#include <ostream>
#include <iomanip>

#include "curvefs/src/client/volume/extent.h"
#include "curvefs/src/volume/common.h"

namespace curvefs {
namespace client {

using ::curvefs::volume::kKiB;
using ::curvefs::volume::kMiB;
using ::curvefs::volume::kGiB;
using ::curvefs::volume::kTiB;

inline bool operator==(const PExtent& p1, const PExtent& p2) {
    return p1.pOffset == p2.pOffset && p1.len == p2.len &&
           p1.UnWritten == p2.UnWritten;
}

inline std::ostream& operator<<(std::ostream& os, const PExtent& p) {
    os << "[physical offset: " << p.pOffset << ", length: " << p.len
       << ", written: " << std::boolalpha << (!p.UnWritten) << "]";

    return os;
}

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_VOLUME_COMMON_H_

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

#ifndef CURVEFS_SRC_VOLUME_UTILS_H_
#define CURVEFS_SRC_VOLUME_UTILS_H_

#include <iosfwd>
#include <vector>

#include "curvefs/src/volume/common.h"

namespace curvefs {
namespace volume {

std::ostream& operator<<(std::ostream& os, const Extent& e);

std::ostream& operator<<(std::ostream& os, const std::vector<Extent>& es);

std::ostream& operator<<(std::ostream& os, const AllocateHint& hint);

std::ostream& operator<<(std::ostream& os, AllocateType type);

}  // namespace volume
}  // namespace curvefs

#endif  // CURVEFS_SRC_VOLUME_UTILS_H_

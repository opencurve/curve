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

/*
 * Project: curve
 * Date: Fri Aug  6 17:10:54 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COMMON_TYPES_H_
#define CURVEFS_SRC_METASERVER_COMMON_TYPES_H_

#include <cstdint>
#include <string>
#include <sstream>
#include <iomanip>

namespace curvefs {
namespace metaserver {
using MetaServerID = uint32_t;
using PoolId = uint32_t;
using CopysetId = uint32_t;
using PartitionId = uint32_t;

inline std::string StringToHex(const std::string &str) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (const auto &c : str) {
        ss << std::setw(2) << static_cast<int>(static_cast<unsigned char>(c));
    }
    return ss.str();
}

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COMMON_TYPES_H_

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
 * Project: Curve
 * Date: 2022-04-08
 * Author: Jingli Chen (Wine93)
 */


#include <random>
#include <sstream>
#include <iomanip>
#include <limits>
#include <iostream>
#include <algorithm>

#include "curvefs/test/metaserver/storage/utils.h"

namespace curvefs {
namespace metaserver {
namespace storage {

std::string RandomString() {
    uint64_t maxUint64 = std::numeric_limits<uint64_t>::max();
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(0, maxUint64);

    std::ostringstream oss;
    oss << std::setw(std::to_string(maxUint64).size())
        << std::setfill('0') << dist(rng);
    return oss.str();
}

std::string RandomStoragePath(const std::string& basedir) {
    return basedir + "_" + RandomString();
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

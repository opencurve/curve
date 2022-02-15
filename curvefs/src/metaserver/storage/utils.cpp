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
 * Date: 2022-02-27
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/metaserver/storage/utils.h"

namespace curvefs {
namespace metaserver {
namespace storage {

std::string EncodeNumber(size_t num) {
    size_t length = sizeof(size_t);
    char buffer[length];
    memcpy(buffer, reinterpret_cast<char*>(&num), length);
    return std::string(buffer, length);
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
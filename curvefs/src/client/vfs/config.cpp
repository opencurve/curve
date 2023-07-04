/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-07-11
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/vfs/config.h"

namespace curvefs {
namespace client {
namespace vfs {

bool Configure::ReadConfig(std::shared_ptr<Reader> in) {
}

bool Configure::Get(const std::string& key, std::string* value) {
}

void Configure::Iterate(IterateHandler handler) {
    for (const auto& item : config_) {
        handler(item.first, item.second);
    }
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

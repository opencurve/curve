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

#ifndef CURVEFS_SRC_CLIENT_VFS_CONFIG_H_
#define CURVEFS_SRC_CLIENT_VFS_CONFIG_H_

#include <map>
#include <regex>
#include <string>
#include <memory>

namespace curvefs {
namespace client {
namespace vfs {

constexpr REG_KEY_VALUE = "^";

class Reader {
 public:
    virtual bool Read(std::string* bytes) = 0;
}

class Configure {
 public:
    using IterateHandler = std::function<void(const std::string& key,
                                              const std::string& value)>;
 public:
    Configure() = default;

    bool ReadConfig(std::shared_ptr<Reader> in);

    bool Get(const std::string& key, std::string* value);

    void Iterate(IterateHandler handler);

 private:
    std::map<std::string, std::string> config_;
};

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VFS_CONFIG_H_

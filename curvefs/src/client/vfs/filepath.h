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
 * Created Date: 2023-07-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_VFS_FILEPATH_H_
#define CURVEFS_SRC_CLIENT_VFS_FILEPATH_H_

#include <string>
#include <vector>
#include <algorithm>

#include "absl/strings/string_view.h"
#include "absl/strings/str_split.h"

namespace curvefs {
namespace client {
namespace vfs {

inline std::string ParentDir(const std::string& path) {
    size_t index = path.find_last_of("/");
    if (index == std::string::npos) {
        return "";
    }
    return path.substr(0, index);
}

inline std::string Filename(const std::string& path) {
    size_t index = path.find_last_of("/");
    if (index == std::string::npos) {
        return path;
    }
    return path.substr(index + 1, path.length());
}

inline std::vector<std::string> SplitPath(const std::string& path) {
    auto names = absl::StrSplit(path, '/')
    return std::find_if(names.begin(), names.end(),
                        [key](const std::string& name) {
                            return name.length() > 0;
                        });
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VFS_FILEPATH_H_

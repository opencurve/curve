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

#include <algorithm>

#include "absl/strings/string_view.h"
#include "absl/strings/str_split.h"
#include "absl/strings/str_join.h"
#include "curvefs/src/client/vfs/utils.h"
#include "curvefs/src/client/logger/error_log.h"

namespace curvefs {
namespace client {
namespace vfs {

namespace strings {

std::string TrimSpace(const std::string& str) {
    std::string s = str;

    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));

    s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), s.end());

    return s;
}

bool HasPrefix(const std::string& str, const std::string& prefix) {
    return str.rfind(prefix, 0) == 0;
}

std::vector<std::string> Split(const std::string& str,
                               const std::string& sep) {
    return absl::StrSplit(str, sep);
}

std::string Join(std::vector<std::string> range, std::string delim) {
    return absl::StrJoin(range, delim);
}

}  // namespace strings

namespace filepath {

std::string ParentDir(const std::string& path) {
    size_t index = path.find_last_of("/");
    if (index == std::string::npos) {
        return "/";
    }
    std::string parent = path.substr(0, index);
    if (parent.size() == 0) {
        return "/";
    }
    return parent;
}

std::string Filename(const std::string& path) {
    size_t index = path.find_last_of("/");
    if (index == std::string::npos) {
        return path;
    }
    return path.substr(index + 1, path.length());
}

std::vector<std::string> Split(const std::string& path) {
    std::vector<std::string> out;
    std::vector<std::string> names = absl::StrSplit(path, '/');
    std::copy_if(names.begin(), names.end(), std::back_inserter(out),
                 [](const std::string& name) {
                     return name.length() > 0;
                 });
    return out;
}

}  // namespace filepath

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

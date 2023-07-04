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

#ifndef CURVEFS_SRC_CLIENT_VFS_UTILS_H_
#define CURVEFS_SRC_CLIENT_VFS_UTILS_H_

#include <string>
#include <vector>

namespace curvefs {
namespace client {
namespace vfs {

namespace strings {

std::string TrimSpace(const std::string& str);

bool HasPrefix(const std::string& str, const std::string& prefix);

std::vector<std::string> Split(const std::string& str, const std::string& sep);

std::string Join(const std::vector<std::string>& range, std::string delim);

std::string Join(const std::vector<std::string>& strs,
                 uint32_t start,
                 uint32_t end,
                 std::string delim);

};  // namespace strings

namespace filepath {

std::string ParentDir(const std::string& path);

std::string Filename(const std::string& path);

std::vector<std::string> Split(const std::string& path);

};  // namespace filepath

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_VFS_UTILS_H_

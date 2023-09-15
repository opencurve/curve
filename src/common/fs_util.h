/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * File Created: 2020-07-03
 * Author: charisu
 */

#ifndef SRC_COMMON_FS_UTIL_H_
#define SRC_COMMON_FS_UTIL_H_

#include <glog/logging.h>
#include <string>
#include <vector>
#include "src/common/string_util.h"

namespace curve {
namespace common {

// Calculate the relative path of path2 relative to path1
inline std::string CalcRelativePath(const std::string &path1,
                                    const std::string &path2) {
    if (path1.empty() || path2.empty()) {
        return "";
    }
    std::vector<std::string> dirs1;
    std::vector<std::string> dirs2;
    SplitString(path1, "/", &dirs1);
    SplitString(path2, "/", &dirs2);
    size_t unmatchedIndex = 0;
    while (unmatchedIndex < dirs1.size() && unmatchedIndex < dirs2.size()) {
        if (dirs1[unmatchedIndex] != dirs2[unmatchedIndex]) {
            break;
        }
        unmatchedIndex++;
    }
    std::string rpath;
    if (unmatchedIndex == dirs1.size()) {
        rpath.append(".");
    }
    for (int i = 0; i < static_cast<int>(dirs1.size() - unmatchedIndex); ++i) {
        if (i > 0) {
            rpath.append("/");
        }
        rpath.append("..");
    }
    for (size_t i = unmatchedIndex; i < dirs2.size(); ++i) {
        rpath.append("/");
        rpath.append(dirs2[i]);
    }
    return rpath;
}

// Check whether the path2 is the subpath of path1
inline bool IsSubPath(const std::string &path1, const std::string &path2) {
    return StringStartWith(CalcRelativePath(path1, path2), "./");
}

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_FS_UTIL_H_

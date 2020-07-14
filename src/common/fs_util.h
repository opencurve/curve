/*
 * Project: curve
 * File Created: 2020-07-03
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#ifndef  SRC_COMMON_FS_UTIL_H_
#define  SRC_COMMON_FS_UTIL_H_

#include <glog/logging.h>
#include <string>
#include <vector>
#include "src/common/string_util.h"

namespace curve {
namespace common {

// 计算path2相对于path1的相对路径
static std::string CalcRelativePath(const std::string& path1,
                                    const std::string& path2) {
    if (path1.empty() || path2.empty()) {
        return "";
    }
    std::vector<std::string> dirs1;
    std::vector<std::string> dirs2;
    SplitString(path1, "/", &dirs1);
    SplitString(path2, "/", &dirs2);
    int unmatchedIndex = 0;
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
    for (int i = 0; i < dirs1.size() - unmatchedIndex; ++i) {
        if (i > 0) {
            rpath.append("/");
        }
        rpath.append("..");
    }
    for (int i = unmatchedIndex; i < dirs2.size(); ++i) {
        rpath.append("/");
        rpath.append(dirs2[i]);
    }
    return rpath;
}


}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_FS_UTIL_H_


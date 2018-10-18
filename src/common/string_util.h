/*
 * Project: curve
 * Created Date: Tuesday September 4th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yanshiguang02@baidu.com

#ifndef  SRC_COMMON_STRING_UTIL_H_
#define  SRC_COMMON_STRING_UTIL_H_

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <vector>

namespace curve {
namespace common {

static inline void SplitString(const std::string& full,
                               const std::string& delim,
                               std::vector<std::string>* result) {
    result->clear();
    if (full.empty()) {
        return;
    }

    std::string tmp;
    std::string::size_type pos_begin = full.find_first_not_of(delim);
    std::string::size_type comma_pos = 0;

    while (pos_begin != std::string::npos) {
        comma_pos = full.find(delim, pos_begin);
        if (comma_pos != std::string::npos) {
            tmp = full.substr(pos_begin, comma_pos - pos_begin);
            pos_begin = comma_pos + delim.length();
        } else {
            tmp = full.substr(pos_begin);
            pos_begin = comma_pos;
        }

        if (!tmp.empty()) {
            result->push_back(tmp);
            tmp.clear();
        }
    }
}

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_STRING_UTIL_H_


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
#include <ctype.h>
#include <glog/logging.h>
#include <string>
#include <vector>

namespace curve {
namespace common {

static void SplitString(const std::string& full,
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

static bool StringToUll(const std::string &value, uint64_t *out) {
    try {
        *out = std::stoull(value);
        return true;
    } catch (std::invalid_argument &e) {
        LOG(ERROR) << "decode string:{" << value << "} to number err:"
                   << e.what();
        return false;
    } catch (std::out_of_range &e) {
        LOG(ERROR) << "decode string:{" << value << "} to number err:"
                   << e.what();
        return false;
    }
}


// 将命名转换为bvar中对应的下划线方式，小写字母和数字不变，大写转小写，其他分隔符转
// 为下划线，驼峰转为下划线
static void ToUnderScored(const std::string &hump, std::string* out) {
    out->clear();
    for (int i = 0; i < hump.size(); ++i) {
        if (isalpha(hump[i])) {
            if (hump[i] < 'a') {  // upper cases
                if (i != 0 && !isupper(hump[i - 1]) && out->back() != '_') {
                    out->push_back('_');
                }
                out->push_back(hump[i] - 'A' + 'a');
            } else {
                out->push_back(hump[i]);
            }
        } else if (isdigit(hump[i])) {
            out->push_back(hump[i]);
        } else if (out->empty() || out->back() != '_') {
            out->push_back('_');
        }
    }
}
}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_STRING_UTIL_H_


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
 * Created Date: Tuesday September 4th 2018
 * Author: hzsunjianliang
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
#include <stdexcept>

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

}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_STRING_UTIL_H_

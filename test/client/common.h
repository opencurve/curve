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
 * File Created: Mon Jul  5 16:55:47 CST 2021
 * Author: wuhanqing
 */

#ifndef TEST_CLIENT_COMMON_H_
#define TEST_CLIENT_COMMON_H_

#include <gflags/gflags.h>

#include <mutex>

namespace brpc {
DECLARE_int32(defer_close_second);
};

namespace curve {
namespace client {

inline void SetSocketDeferCloseSecondIfUnSet() {
    static std::once_flag once;

    std::call_once(once, []() {
        google::CommandLineFlagInfo out;
        google::GetCommandLineFlagInfo("defer_close_second", &out);
        if (out.is_default) {
            brpc::FLAGS_defer_close_second = 120;
        }
    });
}

}  // namespace client
}  // namespace curve

#endif  // TEST_CLIENT_COMMON_H_

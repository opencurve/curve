/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Thursday May 19 10:03:22 CST 2022
 * Author: wuhanqing
 */

#include "src/common/dynamic_vlog.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

DECLARE_int32(v);

namespace curve {
namespace common {

namespace {
bool CheckVLogLevel(const char* /*name*/, int32_t value) {
    FLAGS_v = value;
    LOG(INFO) << "current verbose logging level is `" << FLAGS_v << "`";
    return true;
}
}  // namespace

DEFINE_int32(vlog_level, 0, "set vlog level");
DEFINE_validator(vlog_level, CheckVLogLevel);

}  // namespace common
}  // namespace curve

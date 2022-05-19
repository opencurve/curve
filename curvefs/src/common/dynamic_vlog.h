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
 * Date: Thursday May 19 10:17:45 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_COMMON_DYNAMIC_VLOG_H_
#define CURVEFS_SRC_COMMON_DYNAMIC_VLOG_H_

#include <gflags/gflags.h>

namespace curvefs {
namespace common {

/**
 * use vlog_level to set vlog level on the fly
 * When vlog_level is set, CheckVLogLevel is called to check the validity of the
 * value. Dynamically modify the vlog level by setting FLAG_v in CheckVLogLevel.
 *
 * You can modify the vlog level to 0 using:
 * curl -s http://127.0.0.1:9000/flags/vlog_level?setvalue=0
 */
DECLARE_int32(vlog_level);

}  // namespace common
}  // namespace curvefs

#endif  // CURVEFS_SRC_COMMON_DYNAMIC_VLOG_H_

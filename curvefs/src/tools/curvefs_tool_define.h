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
 * Created Date: 2021-09-14
 * Author: chengyi01
 */

#ifndef CURVEFS_SRC_TOOLS_CURVEFS_TOOLS_DEFINE_H_
#define CURVEFS_SRC_TOOLS_CURVEFS_TOOLS_DEFINE_H_

#include <gflags/gflags.h>

namespace curvefs {
namespace tool {

const char kVersionCmd[] = "version";
const char kConfPathHelp[] = "[-confPath=/etc/curve/tools.conf]";

}  // namespace tool
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_CURVEFS_TOOLS_DEFINE_H_

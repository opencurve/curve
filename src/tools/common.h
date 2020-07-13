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
 * Created Date: 2020-02-06
 * Author: charisu
 */

#ifndef SRC_TOOLS_COMMON_H_
#define SRC_TOOLS_COMMON_H_

#include <gflags/gflags.h>
#include <string>

DECLARE_uint32(logicalPoolId);
DECLARE_uint32(copysetId);

namespace curve {
namespace tool {

/**
 *  @brief 格式化，从metric获取的string
 *         去掉string两边的双引号以及空格和回车
 *  @param[out] str 要格式化的string
 */
void TrimMetricString(std::string* str);

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_COMMON_H_

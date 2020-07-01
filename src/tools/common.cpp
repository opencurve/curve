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

#include "src/tools/common.h"

DEFINE_uint32(logicalPoolId, 0, "logical pool id of copyset");
DEFINE_uint32(copysetId, 0, "copyset id");

namespace curve {
namespace tool {

void TrimMetricString(std::string* str) {
    // 去掉头部空格
    str->erase(0, str->find_first_not_of(" "));
    // 去掉尾部回车
    str->erase(str->find_last_not_of("\r\n") + 1);
    // 去掉两边双引号
    str->erase(0, str->find_first_not_of("\""));
    str->erase(str->find_last_not_of("\"") + 1);
}

}  // namespace tool
}  // namespace curve

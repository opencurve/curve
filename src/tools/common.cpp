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
    //Remove header spaces
    str->erase(0, str->find_first_not_of(" "));
    //Remove the rear carriage return
    str->erase(str->find_last_not_of("\r\n") + 1);
    //Remove double quotes from both sides
    str->erase(0, str->find_first_not_of("\""));
    str->erase(str->find_last_not_of("\"") + 1);
}

bool StringToBool(const std::string& strValue, bool defaultValue) {
    std::string str = strValue;
    transform(str.begin(), str.end(), str.begin(), ::tolower);

    bool istrue = (str == "true") || (str == "yes") || (str == "1");
    bool isfalse = (str == "false") || (str == "no") || (str == "0");
    bool ret = istrue ? true : isfalse ? false : defaultValue;
    return ret;
}

}  // namespace tool
}  // namespace curve

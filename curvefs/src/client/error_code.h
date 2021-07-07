/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef CURVEFS_SRC_CLIENT_ERROR_CODE_H_
#define CURVEFS_SRC_CLIENT_ERROR_CODE_H_

namespace curvefs {
namespace client {

enum class CURVEFS_ERROR {
    OK       = 0,
    FAILED   = 1,
    UNKNOWN  = 2,
    EXISTS   = 3,
    NOTEXIST = 4,
    NO_SPACE = 5,
    BAD_FD   = 6,
};

inline std::ostream& operator<< (std::ostream& os, CURVEFS_ERROR code) {
    os << static_cast<int>(code);
    return os;
}

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_ERROR_CODE_H_

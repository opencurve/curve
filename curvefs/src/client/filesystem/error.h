/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-03-06
 * Author: Jingli Chen (Wine93)
 */

#include <map>
#include <string>
#include <iostream>

#include "curvefs/proto/metaserver.pb.h"

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_ERROR_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_ERROR_H_

namespace curvefs {
namespace client {
namespace filesystem {

using ::curvefs::metaserver::MetaStatusCode;

enum class CURVEFS_ERROR {
    OK = 0,
    INTERNAL = -1,
    UNKNOWN = -2,
    EXISTS = -3,
    NOT_EXIST = -4,
    NO_SPACE = -5,
    BAD_FD = -6,
    INVALID_PARAM = -7,
    NO_PERMISSION = -8,
    NOT_EMPTY = -9,
    NO_FLUSH = -10,
    NOT_SUPPORT = -11,
    NAME_TOO_LONG = -12,
    MOUNT_POINT_EXIST = -13,
    MOUNT_FAILED = -14,
    OUT_OF_RANGE = -15,
    NO_DATA = -16,
    IO_ERROR = -17,
    CACHE_TOO_SMALL = -18,
    STALE = -19,
    NOSYS = -20,
    END_OF_FILE = -21,
};

std::string StrErr(CURVEFS_ERROR code);

int SysErr(CURVEFS_ERROR code);

std::ostream &operator<<(std::ostream &os, CURVEFS_ERROR code);

CURVEFS_ERROR ToFSError(MetaStatusCode code);

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_ERROR_H_

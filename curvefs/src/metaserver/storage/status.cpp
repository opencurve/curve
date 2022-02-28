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
 * Project: Curve
 * Date: 2022-02-14
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/metaserver/storage/status.h"

namespace curvefs {
namespace metaserver {
namespace storage {

std::string Status::ToString() const {
    switch (code_) {
        case Code::kOk:
            return "OK";
        case Code::kDBClosed:
            return "Database Closed";
        case Code::kNotFound:
            return "Not Found";
        case Code::kNotSupported:
            return "Not Supported";
        case Code::kInternalError:
            return "Internal Error";
        default:
            return "Unknown Status";
    }
    return "Unknown Status";
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

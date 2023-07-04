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
 * Created Date: 2023-08-14
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/logger/access_log.h"

namespace curvefs {
namespace client {
namespace logger {

std::shared_ptr<spdlog::logger> Logger;

bool InitAccessLog(const std::string& prefix) {
    std::string filename = StrFormat("%s/access.%d.log", prefix, getpid());
    Logger = spdlog::daily_logger_mt("fuse_access", filename, 0, 0);
    spdlog::flush_every(std::chrono::seconds(1));
    return true;
}

}  // namespace logger
}  // namespace client
}  // namespace curvefs

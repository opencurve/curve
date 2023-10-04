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

#include "src/client/utils.h"

#include <glog/logging.h>
#include <sys/resource.h>

#include <cerrno>

namespace curve {
namespace client {

bool AdjustOpenFileSoftLimitToHardLimit(uint64_t limit) {
    if (limit == 0) {
        return true;
    }

    struct rlimit rlim;
    int rc = getrlimit(RLIMIT_NOFILE, &rlim);

    if (rc != 0) {
        LOG(WARNING) << "getrlimit failed, error: " << strerror(errno);
        return false;
    }

    if (rlim.rlim_max < limit) {
        LOG(WARNING) << "hard limit: " << rlim.rlim_max
                     << " is less than min limit: " << limit;
        return false;
    }

    if (rlim.rlim_cur != rlim.rlim_max) {
        rlim.rlim_cur = rlim.rlim_max;
        rc = setrlimit(RLIMIT_NOFILE, &rlim);
        if (rc != 0) {
            LOG(WARNING) << "setrlimit failed, error: " << strerror(errno);
            return false;
        }
    }

    LOG(INFO) << "set hard limit: " << rlim.rlim_max
              << " soft limit: " << rlim.rlim_cur;
    return true;
}

}  // namespace client
}  // namespace curve

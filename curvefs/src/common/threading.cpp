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
 * Date: Thursday Jun 02 17:32:58 CST 2022
 * Author: wuhanqing
 */

#include "curvefs/src/common/threading.h"

#include <glog/logging.h>
#include <sys/prctl.h>
#include <sys/syscall.h>

namespace curvefs {
namespace common {

namespace {

pid_t gettid() {
    return syscall(SYS_gettid);
}

}  // namespace

void SetThreadName(const char* name) {
    // skip main thread
    if (getpid() == gettid()) {
        return;
    }

    int ret = prctl(PR_SET_NAME, name);

    if (ret != 0) {
        LOG(WARNING) << "Failed to set thread name, tid: " << gettid()
                     << ", error: " << errno;
    }
}

}  // namespace common
}  // namespace curvefs

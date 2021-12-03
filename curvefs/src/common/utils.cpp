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
 * Created Date: Mon Aug 30 2021
 * Author: hzwuhongsong
 */

#include <glog/logging.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "curvefs/src/common/utils.h"

namespace curvefs {
namespace common {

std::string SysUtils::RunSysCmd(const std::string& cmd) {
    char buf[512] = "";
    FILE *fp = NULL;
    if ( (fp = popen(cmd.c_str(), "r")) == NULL ) {
        LOG(ERROR) << "popen failed, errno: " << errno;
        return nullptr;
    }
    std::string output;
    while (fgets(buf, sizeof(buf), fp)) {
        output += buf;
    }
    pclose(fp);
    VLOG(9) << "result: " << output;
    return output;
}

}  // namespace common
}  // namespace curvefs

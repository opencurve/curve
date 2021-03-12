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
 * Created Date: 2020-12-11
 * Author: qinyi
 */

#include "src/chunkserver/watchdog/common.h"

#include <glog/logging.h>
#include <mntent.h>
#include <stdlib.h>
#include <string.h>

#include <fstream>
#include <iostream>
#include <string>

namespace curve {
namespace chunkserver {

using std::string;

int CheckEnv() {
    if (system("smartctl -h > /dev/null")) {
        LOG(ERROR)
            << "Error, smartctl is not available"
            << "Try to run with root privilege, or install smartmontools";
        return -1;
    }

    return 0;
}

string PathToDeviceName(const string* storDir) {
    /* erase last '/' in path */
    if (storDir == nullptr) return "";

    string path = *storDir;
    while (!path.empty() && path.back() == '/') {
        path.pop_back();
    }

    if (path.empty()) return "";

    /* search mount list */
    struct mntent* m;
    FILE* mountFile = setmntent("/proc/mounts", "r");
    if (!mountFile) {
        LOG(ERROR) << "error read /proc/mounts:" << strerror(errno);
        return "";
    }

    string deviceName;
    while ((m = getmntent(mountFile))) {
        if (strstr(m->mnt_dir, path.c_str())) {
            deviceName = m->mnt_fsname;
            break;
        }
    }
    endmntent(mountFile);

    return deviceName;
}

}  // namespace chunkserver
}  // namespace curve

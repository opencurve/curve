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
 * Created Date: 2021-02-24
 * Author: qinyi
 */

#include "src/chunkserver/watchdog/helper.h"

#include <errno.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/stat.h>

#include "src/chunkserver/watchdog/common.h"

namespace curve {
namespace chunkserver {

int WatchdogHelper::Exec(const string& cmd, string* output) {
    PipeGuard pipe(popen(cmd.c_str(), "r"));
    if (pipe == nullptr) {
        LOG_EVERY_N(ERROR, kWatchdoDupLogFreq)
            << "Failed to open pipe for cmd: " << cmd << ", "
            << strerror(errno);
        return -1;
    }

    output->clear();
    char buffer[kWatchdogExecBufferLen];
    while (!feof(pipe)) {
        if (fgets(buffer, kWatchdogExecBufferLen, pipe) != NULL)
            *output += buffer;
    }
    return 0;
}

int WatchdogHelper::Open(const char* file, int flag) {
    return open(file, flag);
}

int WatchdogHelper::Close(int fd) {
    return close(fd);
}
ssize_t WatchdogHelper::Pread(int fd, void* buf, size_t nbytes, off_t offset) {
    return pread(fd, buf, nbytes, offset);
}
ssize_t WatchdogHelper::Pwrite(int fd, const void* buf, size_t n,
                               off_t offset) {
    return pwrite(fd, buf, n, offset);
}

int WatchdogHelper::Access(const char* name, int type) {
    return access(name, type);
}
int WatchdogHelper::Remove(const char* filename) {
    return remove(filename);
}

}  // namespace chunkserver
}  // namespace curve

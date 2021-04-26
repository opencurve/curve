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

#ifndef SRC_CHUNKSERVER_WATCHDOG_HELPER_H_
#define SRC_CHUNKSERVER_WATCHDOG_HELPER_H_

#include <linux/types.h>

#include <string>

using std::string;

namespace curve {
namespace chunkserver {
class WatchdogHelper {
 public:
    virtual ~WatchdogHelper() = default;

    virtual int Exec(const string& cmd, string* output);

    virtual int Open(const char* file, int flag);
    virtual int Close(int fd);
    virtual ssize_t Pread(int fd, void* buf, size_t nbytes, off_t offset);
    virtual ssize_t Pwrite(int fd, const void* buf, size_t n, off_t offset);

    virtual int Access(const char* name, int type);
    virtual int Remove(const char* filename);
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_WATCHDOG_HELPER_H_

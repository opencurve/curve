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
 * Created Date: 2020-09-16
 * Author: charisu
 */

#include "test/chunkserver/raftlog/common.h"

namespace curve {
namespace chunkserver {

int prepare_segment(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDWR|O_CREAT, 0644);
    if (fd < 0) {
        LOG(ERROR) << "Create segment fail";
        return -1;
    }

    if (::fallocate(fd, 0, 0, kSegmentSize) < 0) {
        LOG(ERROR) << "fallocate fail";
        return -1;
    }
    char* data = new char[kSegmentSize];
    memset(data, 0, kSegmentSize);
    if (pwrite(fd, data, kSegmentSize, 0) != kSegmentSize) {
        LOG(ERROR) << "write failed";
        return -1;
    }
    delete data;
    close(fd);
    return 0;
}

}  // namespace chunkserver
}  // namespace curve

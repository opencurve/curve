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
 * Project: nebd
 * Created Date: 2020-02-14
 * Author: charisu
 */

#include "nebd/src/part2/request_executor_ceph.h"

namespace nebd {
namespace server {

std::shared_ptr<NebdFileInstance>
CephRequestExecutor::Open(const std::string& filename) {
    return nullptr;
}

std::shared_ptr<NebdFileInstance>
CephRequestExecutor::Reopen(const std::string& filename,
                            const ExtendAttribute& xattr) {
    return nullptr;
}

int CephRequestExecutor::Close(NebdFileInstance* fd) {
    return 0;
}

int CephRequestExecutor::Extend(NebdFileInstance* fd,
                                int64_t newsize) {
    return 0;
}

int CephRequestExecutor::Discard(NebdFileInstance* fd,
                                 NebdServerAioContext* aioctx) {
    return 0;
}

int CephRequestExecutor::AioRead(NebdFileInstance* fd,
                                 NebdServerAioContext* aioctx) {
    return 0;
}

int CephRequestExecutor::AioWrite(NebdFileInstance* fd,
                                  NebdServerAioContext* aioctx) {
    return 0;
}

int CephRequestExecutor::Flush(NebdFileInstance* fd,
                               NebdServerAioContext* aioctx) {
    return 0;
}

int CephRequestExecutor::GetInfo(NebdFileInstance* fd,
                                 NebdFileInfo* fileInfo) {
    return 0;
}

int CephRequestExecutor::InvalidCache(NebdFileInstance* fd) {
    return 0;
}


}  // namespace server
}  // namespace nebd

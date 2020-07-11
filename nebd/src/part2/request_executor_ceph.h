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

#ifndef NEBD_SRC_PART2_REQUEST_EXECUTOR_CEPH_H_
#define NEBD_SRC_PART2_REQUEST_EXECUTOR_CEPH_H_

#include <string>
#include <memory>
#include "nebd/src/part2/request_executor.h"

namespace nebd {
namespace server {

class CephFileInstance : public NebdFileInstance {
 public:
    CephFileInstance() {}
    ~CephFileInstance() {}
};

class CephRequestExecutor : public NebdRequestExecutor {
 public:
    static CephRequestExecutor& GetInstance() {
        static CephRequestExecutor executor;
        return executor;
    }
    ~CephRequestExecutor() {}
    std::shared_ptr<NebdFileInstance> Open(const std::string& filename) override;  // NOLINT
    std::shared_ptr<NebdFileInstance> Reopen(
        const std::string& filename, const ExtendAttribute& xattr) override;
    int Close(NebdFileInstance* fd) override;
    int Extend(NebdFileInstance* fd, int64_t newsize) override;
    int GetInfo(NebdFileInstance* fd, NebdFileInfo* fileInfo) override;
    int Discard(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int AioRead(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int AioWrite(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int Flush(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int InvalidCache(NebdFileInstance* fd) override;

 private:
    CephRequestExecutor() {}
};

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_REQUEST_EXECUTOR_CEPH_H_

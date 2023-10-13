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
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 */

#ifndef NEBD_SRC_PART2_REQUEST_EXECUTOR_H_
#define NEBD_SRC_PART2_REQUEST_EXECUTOR_H_

#include <map>
#include <memory>
#include <string>

#include "nebd/src/part2/define.h"

namespace nebd {
namespace client {
class ProtoOpenFlags;
}  // namespace client
}  // namespace nebd

namespace nebd {
namespace server {

class CurveRequestExecutor;

using OpenFlags = nebd::client::ProtoOpenFlags;

// The file instance context information used in the specific RequestExecutor
// The file context information required for RequestExecutor is recorded in
// FileInstance
class NebdFileInstance {
 public:
    NebdFileInstance() {}
    virtual ~NebdFileInstance() {}
    // The content that needs to be persisted to the file is returned in kv
    // format, such as the sessionid returned when curve open This content will
    // also be used when reopening files
    ExtendAttribute xattr;
};

class NebdRequestExecutor {
 public:
    NebdRequestExecutor() {}
    virtual ~NebdRequestExecutor() {}
    virtual std::shared_ptr<NebdFileInstance> Open(
        const std::string& filename, const OpenFlags* openflags) = 0;
    virtual std::shared_ptr<NebdFileInstance> Reopen(
        const std::string& filename, const ExtendAttribute& xattr) = 0;
    virtual int Close(NebdFileInstance* fd) = 0;
    virtual int Extend(NebdFileInstance* fd, int64_t newsize) = 0;
    virtual int GetInfo(NebdFileInstance* fd, NebdFileInfo* fileInfo) = 0;
    virtual int Discard(NebdFileInstance* fd, NebdServerAioContext* aioctx) = 0;
    virtual int AioRead(NebdFileInstance* fd, NebdServerAioContext* aioctx) = 0;
    virtual int AioWrite(NebdFileInstance* fd,
                         NebdServerAioContext* aioctx) = 0;  // NOLINT
    virtual int Flush(NebdFileInstance* fd, NebdServerAioContext* aioctx) = 0;
    virtual int InvalidCache(NebdFileInstance* fd) = 0;
};

class NebdRequestExecutorFactory {
 public:
    NebdRequestExecutorFactory() = default;
    ~NebdRequestExecutorFactory() = default;

    static NebdRequestExecutor* GetExecutor(NebdFileType type);
};

class NebdFileInstanceFactory {
 public:
    NebdFileInstanceFactory() = default;
    ~NebdFileInstanceFactory() = default;

    static NebdFileInstancePtr GetInstance(NebdFileType type);
};

// for test
extern NebdRequestExecutor* g_test_executor;

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_REQUEST_EXECUTOR_H_

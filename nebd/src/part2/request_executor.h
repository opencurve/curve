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
#include <string>
#include <memory>
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

// 具体RequestExecutor中会用到的文件实例上下文信息
// RequestExecutor需要用到的文件上下文信息都记录到FileInstance内
class NebdFileInstance {
 public:
    NebdFileInstance() {}
    virtual ~NebdFileInstance() {}
    // 需要持久化到文件的内容，以kv形式返回，例如curve open时返回的sessionid
    // 文件reopen的时候也会用到该内容
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
    virtual int AioWrite(NebdFileInstance* fd, NebdServerAioContext* aioctx) = 0;  // NOLINT
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

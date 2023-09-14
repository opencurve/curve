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
 * Created Date: Tuesday February 11th 2020
 * Author: yangyaokai
 */

#ifndef NEBD_SRC_PART2_DEFINE_H_
#define NEBD_SRC_PART2_DEFINE_H_

#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <string>
#include <memory>
#include <map>

#include "nebd/src/common/rw_lock.h"

namespace nebd {
namespace server {

using nebd::common::RWLock;
using ::google::protobuf::Message;
using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;

const char CURVE_PREFIX[] = "cbd";
const char TEST_PREFIX[] = "test";

// Types of nebd asynchronous requests
enum class LIBAIO_OP {
    LIBAIO_OP_READ,
    LIBAIO_OP_WRITE,
    LIBAIO_OP_DISCARD,
    LIBAIO_OP_FLUSH,
    LIBAIO_OP_UNKNOWN,
};

enum class NebdFileStatus {
    OPENED = 0,
    CLOSED = 1,
    DESTROYED = 2,
};

enum class NebdFileType {
    CURVE = 1,
    TEST = 2,
    UNKWOWN = 3,
};

class NebdFileInstance;
class NebdRequestExecutor;
using NebdFileInstancePtr = std::shared_ptr<NebdFileInstance>;
using RWLockPtr = std::shared_ptr<RWLock>;

struct NebdServerAioContext;

// The type of nebd callback function
typedef void (*NebdAioCallBack)(struct NebdServerAioContext* context);

// Context of Nebd server-side asynchronous requests
// Record the type, parameters, return information, and rpc information of the request
struct NebdServerAioContext {
    // Requested offset
    off_t offset = 0;
    // Requested size
    size_t size = 0;
    // Record the return value returned asynchronously
    int ret = -1;
    // The type of asynchronous request, as defined in the definition
    LIBAIO_OP op = LIBAIO_OP::LIBAIO_OP_UNKNOWN;
    // Callback function called at the end of asynchronous request
    NebdAioCallBack cb;
    // Buf requested
    void* buf = nullptr;
    // The corresponding content of the rpc request
    Message* response = nullptr;
    // Callback function for rpc requests
    Closure *done = nullptr;
    // Controller for rpc requests
    RpcController* cntl = nullptr;
    // return rpc when io error
    bool returnRpcWhenIoError = false;
};

struct NebdFileInfo {
    // File size
    uint64_t size;
    // object/chunk size
    uint64_t obj_size;
    // Number of objects
    uint64_t num_objs;
    // block size
    uint32_t block_size;
};

using ExtendAttribute = std::map<std::string, std::string>;
// Metadata information for file persistence on the Nebd server side
struct NebdFileMeta {
    int fd;
    std::string fileName;
    ExtendAttribute xattr;
};

// part2 Configuration Item
const char LISTENADDRESS[] = "listen.address";
const char METAFILEPATH[] = "meta.file.path";
const char HEARTBEATTIMEOUTSEC[] = "heartbeat.timeout.sec";
const char HEARTBEATCHECKINTERVALMS[] = "heartbeat.check.interval.ms";
const char CURVECLIENTCONFPATH[] = "curveclient.confPath";
const char RESPONSERETURNRPCWHENIOERROR[] = "response.returnRpcWhenIoError";

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_DEFINE_H_

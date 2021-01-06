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

// nebd异步请求的类型
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

// nebd回调函数的类型
typedef void (*NebdAioCallBack)(struct NebdServerAioContext* context);

// nebd server端异步请求的上下文
// 记录请求的类型、参数、返回信息、rpc信息
struct NebdServerAioContext {
    // 请求的offset
    off_t offset = 0;
    // 请求的size
    size_t size = 0;
    // 记录异步返回的返回值
    int ret = -1;
    // 异步请求的类型，详见定义
    LIBAIO_OP op = LIBAIO_OP::LIBAIO_OP_UNKNOWN;
    // 异步请求结束时调用的回调函数
    NebdAioCallBack cb;
    // 请求的buf
    void* buf = nullptr;
    // rpc请求的相应内容
    Message* response = nullptr;
    // rpc请求的回调函数
    Closure *done = nullptr;
    // rpc请求的controller
    RpcController* cntl = nullptr;
    // return rpc when io error
    bool returnRpcWhenIoError = false;
};

struct NebdFileInfo {
    // 文件大小
    uint64_t size;
    // object/chunk大小
    uint64_t obj_size;
    // object数量
    uint64_t num_objs;
};

using ExtendAttribute = std::map<std::string, std::string>;
// nebd server 端文件持久化的元数据信息
struct NebdFileMeta {
    int fd;
    std::string fileName;
    ExtendAttribute xattr;
};

// part2配置项
const char LISTENADDRESS[] = "listen.address";
const char METAFILEPATH[] = "meta.file.path";
const char HEARTBEATTIMEOUTSEC[] = "heartbeat.timeout.sec";
const char HEARTBEATCHECKINTERVALMS[] = "heartbeat.check.interval.ms";
const char CURVECLIENTCONFPATH[] = "curveclient.confPath";
const char RESPONSERETURNRPCWHENIOERROR[] = "response.returnRpcWhenIoError";

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_DEFINE_H_

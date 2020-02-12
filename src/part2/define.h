/*
 * Project: nebd
 * Created Date: Tuesday February 11th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_DEFINE_H_
#define SRC_PART2_DEFINE_H_

#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <string>
#include <memory>

#include "src/common/rw_lock.h"

namespace nebd {
namespace server {

using nebd::common::RWLock;
using ::google::protobuf::Message;
using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;

const char CURVE_PREFIX[] = "cbd";
const char CEPH_PREFIX[] = "rbd";
const char TEST_PREFIX[] = "test";

// nebd异步请求的类型
enum class LIBAIO_OP {
    LIBAIO_OP_READ,
    LIBAIO_OP_WRITE,
    LIBAIO_OP_DISCARD,
    LIBAIO_OP_FLUSH,
};

enum class NebdFileStatus {
    OPENED = 0,
    CLOSED = 1,
};

enum class NebdFileType {
    CEPH = 0,
    CURVE = 1,
    TEST = 2,
    UNKWOWN = 3,
};

class NebdFileInstance;
class NebdRequestExecutor;
using NebdFileInstancePtr = std::shared_ptr<NebdFileInstance>;
using RWLockPtr = std::shared_ptr<RWLock>;

// nebd server记录的文件信息内存结构
struct NebdFileRecord {
    // 文件读写锁，处理请求前加读锁，close文件的时候加写锁
    // 避免close时还有请求未处理完
    RWLockPtr rwLock = std::make_shared<RWLock>();
    // nebd server为该文件分配的唯一标识符
    int fd = 0;
    // 文件名称
    std::string fileName = "";
    // 文件当前状态，opened表示文件已打开，closed表示文件已关闭
    NebdFileStatus status = NebdFileStatus::CLOSED;
    // 该文件上一次收到心跳时的时间戳
    uint64_t timeStamp = 0;
    // 文件在executor open时返回上下文信息，用于后续文件的请求处理
    NebdFileInstancePtr fileInstance = nullptr;
    // 文件对应的executor的指针
    NebdRequestExecutor* executor = nullptr;
};
using NebdFileRecordPtr = std::shared_ptr<NebdFileRecord>;

struct NebdServerAioContext;

// nebd回调函数的类型
typedef void (*NebdAioCallBack)(struct NebdServerAioContext* context);

// nebd server端异步请求的上下文
// 记录请求的类型、参数、返回信息、rpc信息
struct NebdServerAioContext {
    // 请求的offset
    off_t offset;
    // 请求的size
    size_t size;
    // 记录异步返回的返回值
    int ret;
    // 异步请求的类型，详见定义
    LIBAIO_OP op;
    // 异步请求结束时调用的回调函数
    NebdAioCallBack cb;
    // 请求的buf
    void* buf;
    // rpc请求的相应内容
    Message* response;
    // rpc请求的回调函数
    Closure *done;
    // rpc请求的controller
    RpcController* cntl;
};

struct NebdFileInfo {
    // 文件大小
    uint64_t size;
    // object大小（ceph为object，curve为chunk）
    uint64_t obj_size;
    // object数量
    uint64_t num_objs;
};

// part2配置项
const char LISTENADDRESS[] = "listen.address";
const char METAFILEPATH[] = "meta.file.path";
const char HEARTBEATTIMEOUTSEC[] = "heartbeat.timeout.sec";
const char HEARTBEATCHECKINTERVALMS[] = "heartbeat.check.interval.ms";

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_DEFINE_H_

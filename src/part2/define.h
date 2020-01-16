#ifndef SRC_PART2_DEFINE_H_
#define SRC_PART2_DEFINE_H_

#include <google/protobuf/message.h>
#include <string>

#include "src/common/rw_lock.h"

using nebd::common::RWLock;
using ::google::protobuf::Message;
using ::google::protobuf::Closure;

namespace nebd {
namespace server {

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
};

class NebdFileInstance;
class NebdRequestExecutor;

struct NebdFileInfo {
    int fd;
    std::string fileName;
    NebdFileType type;
    NebdFileStatus status;
    uint64_t timeStamp;
    RWLock rwLock;
    NebdFileInstance* fileInstance;
    NebdRequestExecutor* executor;
};

struct NebdServerAioContext;

// nebd回调函数的类型
typedef void (*NebdAioCallBack)(struct NebdServerAioContext* context);

typedef struct NebdServerAioContext {
    off_t offset;             // 请求的offset
    size_t length;            // 请求的length
    int ret;                  // 记录异步返回的返回值
    LIBAIO_OP op;             // 异步请求的类型，详见定义
    NebdAioCallBack cb;       // 异步请求的回调函数
    void* buf;                // 请求的buf
    Message* response;        // 请求返回内容
    Closure *done;            // 请求回调函数
} ClientAioContext;

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_DEFINE_H_

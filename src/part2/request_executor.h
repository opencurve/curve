#ifndef SRC_PART2_REQUEST_EXECUTOR_H_
#define SRC_PART2_REQUEST_EXECUTOR_H_

#include "src/part2/define.h"

namespace nebd {
namespace server {

class NebdFileInstance {
 public:
    NebdFileInstance() {}
    virtual ~NebdFileInstance() {}
};

class NebdRequestExcutor {
 public:
    NebdRequestExcutor() {}
    virtual ~NebdRequestExcutor() {}
    virtual std::shared_ptr<NebdFileInstance> Open(const std::string& filename) = 0;  // NOLINT
    virtual int Close(NebdFileInstance* fd) = 0;
    virtual int Extend(NebdFileInstance* fd, int64_t newsize) = 0;
    virtual int StatFile(NebdFileInstance* fd) = 0;
    virtual int Discard(NebdFileInstance* fd, NebdServerAioContext* aioctx) = 0;
    virtual int AioRead(NebdFileInstance* fd, NebdServerAioContext* aioctx) = 0;
    virtual int AioWrite(NebdFileInstance* fd, NebdServerAioContext* aioctx) = 0;
    virtual int Flush(NebdFileInstance* fd, NebdServerAioContext* aioctx) = 0;
    virtual int GetInfo(NebdFileInstance* fd) = 0;
    virtual int InvalidCache(NebdFileInstance* fd) = 0;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_REQUEST_EXECUTOR_H_

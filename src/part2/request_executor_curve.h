#ifndef SRC_PART2_REQUEST_EXECUTO_CURVE_H_
#define SRC_PART2_REQUEST_EXECUTO_CURVE_H_

#include <string>
#include <memory>
#include "src/part2/request_executor.h"

namespace nebd {
namespace server {

class CurveFileInstance : public NebdFileInstance {
 public:
    CurveFileInstance() {}
    ~CurveFileInstance() {}

    int fd = 0;
};

class CurveRequestExecutor : public NebdRequestExecutor {
 public:
    static CurveRequestExecutor& GetInstance() {
        static CurveRequestExecutor executor;
        return executor;
    }
    ~CurveRequestExecutor() {}
    std::shared_ptr<NebdFileInstance> Open(const std::string& filename) override;  // NOLINT
    std::shared_ptr<NebdFileInstance> Reopen(
        const std::string& filename, AdditionType addtion) override;
    int Close(NebdFileInstance* fd) override;
    int Extend(NebdFileInstance* fd, int64_t newsize) override;
    int GetInfo(NebdFileInstance* fd, NebdFileInfo* fileInfo) override;
    int StatFile(NebdFileInstance* fd, NebdFileInfo* fileInfo) override;
    int Discard(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int AioRead(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int AioWrite(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int Flush(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int InvalidCache(NebdFileInstance* fd) override;

 private:
    CurveRequestExecutor() {}
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_REQUEST_EXECUTO_CURVE_H_

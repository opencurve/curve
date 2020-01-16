#ifndef SRC_PART2_REQUEST_EXECUTO_CEPH_H_
#define SRC_PART2_REQUEST_EXECUTO_CEPH_H_

#include <string>
#include <memory>
#include "src/part2/request_executor.h"

namespace nebd {
namespace server {

class CephFileInstance : public NebdFileInstance {
 public:
    CephFileInstance() {}
    ~CephFileInstance() {}

    int fd;
    std::string sessionid;
};

class CephRequestExcutor : public NebdRequestExcutor {
 public:
    CephRequestExcutor() {}
    ~CephRequestExcutor() {}
    std::shared_ptr<NebdFileInstance> Open(const std::string& filename) override;  // NOLINT
    int Close(NebdFileInstance* fd) override;
    int Extend(NebdFileInstance* fd, int64_t newsize) override;
    int StatFile(NebdFileInstance* fd) override;
    int Discard(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int AioRead(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int AioWrite(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int Flush(NebdFileInstance* fd, NebdServerAioContext* aioctx) override;
    int GetInfo(NebdFileInstance* fd) override;
    int InvalidCache(NebdFileInstance* fd) override;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_REQUEST_EXECUTO_CEPH_H_

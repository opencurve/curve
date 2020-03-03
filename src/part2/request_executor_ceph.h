/*
 * Project: nebd
 * Created Date: 2020-02-14
 * Author: charisu
 * Copyright (c) 2020 netease
 */

#ifndef SRC_PART2_REQUEST_EXECUTOR_CEPH_H_
#define SRC_PART2_REQUEST_EXECUTOR_CEPH_H_

#include <string>
#include <memory>
#include "src/part2/request_executor.h"

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

#endif  // SRC_PART2_REQUEST_EXECUTOR_CEPH_H_

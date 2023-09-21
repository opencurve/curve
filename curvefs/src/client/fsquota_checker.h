#ifndef CURVEFS_SRC_CLIENT_FUSEQUOTA_CHECKER_H_
#define CURVEFS_SRC_CLIENT_FUSEQUOTA_CHECKER_H_

#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include <brpc/periodic_task.h>

namespace curvefs {
namespace client {

class FsQuotaChecker {
 public:
    static FsQuotaChecker &GetInstance() {
        static FsQuotaChecker instance_;
        return instance_;
    }

    void Init(uint32_t fsId, std::shared_ptr<rpcclient::MdsClient> mdsClient,
              std::shared_ptr<rpcclient::MetaServerClient> metaserverClient);

    bool QuotaBytesCheck(uint64_t incBytes);

    void UpdateQuotaCache();

 private:
    uint32_t fsId_;
    std::atomic<uint64_t> fsCapacityCache_;
    std::atomic<uint64_t> fsUsedBytesCache_;
    std::shared_ptr<rpcclient::MdsClient> mdsClient_;
    std::shared_ptr<rpcclient::MetaServerClient> metaserverClient_;
};

class UpdateQuotaCacheTask : public brpc::PeriodicTask {
 public:
    explicit UpdateQuotaCacheTask(FsQuotaChecker *fsQuotaChecker,
                                  int64_t interval_s)
        : fsQuotaChecker_(fsQuotaChecker), interval_s_(interval_s) {}
    bool OnTriggeringTask(timespec *next_abstime) override;
    void OnDestroyingTask() override;

 private:
    FsQuotaChecker *fsQuotaChecker_;
    int64_t interval_s_;
};


void StartUpdateQuotaCacheTask(FsQuotaChecker *updater, int64_t interval_s);


}  // namespace client

}  // namespace curvefs


#endif  // CURVEFS_SRC_CLIENT_FUSEQUOTA_CHECKER_H_
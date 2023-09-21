#ifndef CURVEFS_SRC_METASERVER_FSUSED_MANAGER_H_
#define CURVEFS_SRC_METASERVER_FSUSED_MANAGER_H_

#include <deque>
#include <memory>
#include <brpc/periodic_task.h>
#include <bthread/mutex.h>
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/proto/metaserver.pb.h"

namespace curvefs {
namespace metaserver {

using curvefs::client::rpcclient::MetaServerClient;

// fs level used manager
class FsUsedManager {
 public:
    explicit FsUsedManager(std::shared_ptr<MetaServerClient> metaserverClient)
        : metaserverClient_(metaserverClient), fsUsedDeltas_() {}

    void AddFsUsedDelta(FsUsedDelta &&fsUsedDelta);
    void ApplyFsUsedDeltas();

 private:
    std::shared_ptr<MetaServerClient> metaserverClient_;
    std::deque<FsUsedDelta> fsUsedDeltas_;
    bthread::Mutex dirtyLock_;
};

class UpdateFsUsedTask : public brpc::PeriodicTask {
 public:
    explicit UpdateFsUsedTask(FsUsedManager *fsUsedManager, int64_t interval_s)
        : fsUsedManager_(fsUsedManager), interval_s_(interval_s) {}
    bool OnTriggeringTask(timespec *next_abstime) override;
    void OnDestroyingTask() override;

 private:
    FsUsedManager *fsUsedManager_;
    int64_t interval_s_;
};

void StartUpdateFsUsedTask(FsUsedManager *fsUsedManager, int64_t interval_s);

}  // namespace metaserver
}  // namespace curvefs


#endif  // CURVEFS_SRC_METASERVER_FSUSED_MANAGER_H_

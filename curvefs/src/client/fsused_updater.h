#ifndef CURVEFS_SRC_CLIENT_FUSEUSED_UPDATER_H_
#define CURVEFS_SRC_CLIENT_FUSEUSED_UPDATER_H_

#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include <brpc/periodic_task.h>


namespace curvefs {
namespace client {

class FsUsedUpdater {
 public:
    static FsUsedUpdater &GetInstance() {
        static FsUsedUpdater instance_;
        return instance_;
    }

    void Init(uint32_t fsId,
              std::shared_ptr<rpcclient::MetaServerClient> metaserverClient) {
        fsId_ = fsId;
        metaserverClient_ = metaserverClient;
        deltaBytes_.store(0);
    }

    void UpdateDeltaBytes(int64_t deltaBytes);

    void UpdateFsUsed();

 private:
    uint32_t fsId_;
    std::atomic<int64_t> deltaBytes_;
    std::shared_ptr<rpcclient::MetaServerClient> metaserverClient_;
};

class UpdateFsUsedTask : public brpc::PeriodicTask {
 public:
    explicit UpdateFsUsedTask(FsUsedUpdater *fsUsedUpdater, int64_t interval_s)
        : fsUsedUpdater_(fsUsedUpdater), interval_s_(interval_s) {}
    bool OnTriggeringTask(timespec *next_abstime) override;
    void OnDestroyingTask() override;

 private:
    FsUsedUpdater *fsUsedUpdater_;
    int64_t interval_s_;
};


void StartUpdateFsUsedTask(FsUsedUpdater *updater, int64_t interval_s);

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FUSEUSED_UPDATER_H_

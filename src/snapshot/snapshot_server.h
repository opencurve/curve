/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_SNAPSHOT_SNAPSHOT_SERVER_H
#define CURVE_SRC_SNAPSHOT_SNAPSHOT_SERVER_H



#include <brpc/server.h>

#include <memory>
#include <atomic>
#include <string>


#include "src/snapshot/snapshot_service.h"
#include "src/snapshot/snapshot_service_manager.h"

namespace curve {
namespace snapshotserver {

struct SnapshotServerOption {
    std::string listenAddr;
};

class SnapshotServer {
 public:
    SnapshotServer() {}

    int Init();
    int Start();
    int Stop();

    void RunUntilAskedToQuit();

 private:
    std::shared_ptr<brpc::Server> server_;
    std::shared_ptr<SnapshotServiceImpl> service_;
    std::shared_ptr<SnapshotServiceManager> serviceManager_;

    SnapshotServerOption options_;
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOT_SERVER_H

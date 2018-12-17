/*
 * Project: curve
 * Created Date: Sat Jan 05 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_TEST_SNAPSHOT_FAKE_SNAPSHOT_SERVER_H_
#define CURVE_TEST_SNAPSHOT_FAKE_SNAPSHOT_SERVER_H_

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
    ~SnapshotServer() {}

    int Init();
    int Start();
    int Stop();

 private:
    std::shared_ptr<brpc::Server> server_;
    std::shared_ptr<SnapshotServiceImpl> service_;
    std::shared_ptr<SnapshotServiceManager> serviceManager_;

    SnapshotServerOption options_;
};

}  // namespace snapshotserver
}  // namespace curve


#endif  // CURVE_TEST_SNAPSHOT_FAKE_SNAPSHOT_SERVER_H_

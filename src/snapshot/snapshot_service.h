/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_SNAPSHOT_SNAPSHOT_SERVICE_H
#define CURVE_SRC_SNAPSHOT_SNAPSHOT_SERVICE_H

#include <brpc/server.h>
#include <memory>

#include "proto/snapshotserver.pb.h"
#include "src/snapshot/snapshot_service_manager.h"

namespace curve {
namespace snapshotserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class SnapshotServiceImpl : public SnapshotService {
 public:
    explicit SnapshotServiceImpl(
        std::shared_ptr<SnapshotServiceManager> manager)
        : manager_(manager) {}
    virtual ~SnapshotServiceImpl() {}
    void default_method(RpcController* cntl,
                        const HttpRequest* req,
                        HttpResponse* resp,
                        Closure* done);
 private:
    std::shared_ptr<SnapshotServiceManager> manager_;
};

}  // namespace snapshotserver
}  // namespace curve

#endif  // CURVE_SRC_SNAPSHOT_SNAPSHOT_SERVICE_H

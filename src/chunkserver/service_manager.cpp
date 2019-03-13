/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "src/chunkserver/service_manager.h"

namespace curve {
namespace chunkserver {

int ServiceManager::Init(const ServiceOptions &options) {
    int ret = 0;
    options_ = options;
    chunkserver_ = options.chunkserver;
    copysetNodeManager_ = options.copysetNodeManager;
    cloneManager_ = options.cloneManager;

    butil::ip_t ip;
    if (butil::str2ip(options_.ip.c_str(), &ip) < 0) {
        LOG(ERROR) << "Invalid server IP provided: " << options_.ip;
        return -1;
    }
    if (options_.port <= 0 || options_.port >= 65535) {
        LOG(ERROR) << "Invalid server port provided: " << options_.port;
        return -1;
    }

    endPoint_ = butil::EndPoint(ip, options_.port);

    ChunkServiceOptions chunkServiceOptions;
    chunkServiceOptions.copysetNodeManager = copysetNodeManager_;
    chunkServiceOptions.cloneManager = cloneManager_;

    server_ = new brpc::Server();

    chunkserverService_ = new ChunkServerServiceImpl(chunkserver_);
    CHECK(nullptr != chunkserverService_) << "Fail to new ChunkServerService";
    copysetService_ = new CopysetServiceImpl(copysetNodeManager_);
    CHECK(nullptr != copysetService_) << "Fail to new CopysetService";
    chunkService_ = new ChunkServiceImpl(chunkServiceOptions);
    CHECK(nullptr != chunkService_) << "Fail to new ChunkService";
    braftCliService_ = new BRaftCliServiceImpl();
    CHECK(nullptr != braftCliService_) << "Fail to new BRaftCliService";
    raftService_ = new braft::RaftServiceImpl(endPoint_);
    CHECK(nullptr != raftService_) << "Fail to new RaftService";
    raftStatService_ = new braft::RaftStatImpl();
    CHECK(nullptr != raftStatService_) << "Fail to new RaftStatService";

    ret = server_->AddService(chunkserverService_,
                              brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add ChunkServerService";

    ret = server_->AddService(copysetService_,
                              brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add CopysetService";

    ret = server_->AddService(chunkService_,
                              brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add ChunkService";

    ret = server_->AddService(braftCliService_,
                              brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add BRaftCliService";

    ret = server_->AddService(raftService_,
                              brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add RaftService";

    ret = server_->AddService(raftStatService_,
                              brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add RaftStatService";

    ret = server_->AddService(braft::file_service(),
                              brpc::SERVER_DOESNT_OWN_SERVICE);
    CHECK(0 == ret) << "Fail to add FileService";

    if (!braft::NodeManager::GetInstance()->server_exists(endPoint_)) {
        braft::NodeManager::GetInstance()->add_address(endPoint_);
    }

    return 0;
}

int ServiceManager::Run() {
    LOG(INFO) << "RPC server is going to serve on: " << options_.ip << ":"
              << options_.port;
    if (server_->Start(endPoint_, NULL) != 0) {
        LOG(ERROR) << "Fail to start RPC Server";
        return -1;
    }

    return 0;
}

int ServiceManager::Fini() {
    server_->Stop(0);
    server_->Join();

    // TODO(wenyu): remove services

    delete chunkService_;
    delete copysetService_;
    delete chunkserverService_;
    delete braftCliService_;
    delete raftService_;
    delete raftStatService_;
    delete server_;

    return 0;
}

}  // namespace chunkserver
}  // namespace curve

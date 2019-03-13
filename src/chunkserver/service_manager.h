/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include <brpc/server.h>

#include <braft/file_service.h>
#include <braft/node_manager.h>
#include <braft/builtin_service_impl.h>

#include <string>

#include "src/chunkserver/chunk_service.h"
#include "src/chunkserver/copyset_service.h"
#include "src/chunkserver/chunkserver_service.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/braft_cli_service.h"
#include "src/chunkserver/clone_manager.h"

#include "proto/chunk.pb.h"
#include "proto/copyset.pb.h"
#include "proto/chunkserver.pb.h"
#include "proto/configuration.pb.h"

#ifndef SRC_CHUNKSERVER_SERVICE_MANAGER_H_
#define SRC_CHUNKSERVER_SERVICE_MANAGER_H_

namespace curve {
namespace chunkserver {

class ChunkServer;

struct ServiceOptions {
    std::string             ip;
    int                     port;
    ChunkServer*            chunkserver;
    CopysetNodeManager*     copysetNodeManager;
    CloneManager*           cloneManager;
};

class ServiceManager {
 public:
    ServiceManager() {}
    ~ServiceManager() {}

    int Init(const ServiceOptions &options);
    int Run();
    int Fini();

 private:
    butil::EndPoint         endPoint_;
    brpc::Server*           server_;

    ServiceOptions          options_;

    ChunkServiceImpl*       chunkService_;
    CopysetServiceImpl*     copysetService_;
    ChunkServerServiceImpl* chunkserverService_;
    BRaftCliServiceImpl*    braftCliService_;
    braft::RaftServiceImpl* raftService_;
    braft::RaftStatImpl*    raftStatService_;

    ChunkServer*            chunkserver_;
    CopysetNodeManager*     copysetNodeManager_;
    CloneManager*           cloneManager_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_SERVICE_MANAGER_H_

/*
 * Project: curve
 * Created Date: Friday October 19th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <brpc/channel.h>
#include <brpc/server.h>

#include "test/mds/nameserver2/fakes.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/nameserver2/namespace_service.h"
#include "src/mds/nameserver2/curvefs.h"
#include "src/mds/topology/topology_admin.h"


DEFINE_string(listenAddr, ":6666", "Initial  mds listen addr");

namespace curve {
namespace mds {
int main() {
    // TODO(xuchaojie): init topolgy

    // init nameserver
    NameServerStorage *storage_;
    InodeIDGenerator *inodeGenerator_;
    ChunkSegmentAllocator *chunkSegmentAllocate_;

    storage_ =  new FakeNameServerStorage();
    inodeGenerator_ = new FakeInodeIDGenerator(0);

    // TODO(xuchaojie): use read topology Admin
    std::shared_ptr<FackTopologyAdmin> topologyAdmin =
                                std::make_shared<FackTopologyAdmin>();
    std::shared_ptr<FackChunkIDGenerator> chunkIdGenerator =
                            std::make_shared<FackChunkIDGenerator>();
    chunkSegmentAllocate_ =
                new ChunkSegmentAllocatorImpl(topologyAdmin, chunkIdGenerator);
    kCurveFS.Init(storage_, inodeGenerator_, chunkSegmentAllocate_);

    // add rpc service
    brpc::Server server;
    NameSpaceService namespaceService;
    if (server.AddService(&namespaceService,
        brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "add namespaceService error";
        return -1;
    }
    // TODO(xuchaojie): add topology service

    // start rpc server
    brpc::ServerOptions option;
    option.idle_timeout_sec = -1;
    if (server.Start(FLAGS_listenAddr.c_str(), &option) != 0) {
        LOG(ERROR) << "start brpc server error";
    }

    server.Join();
    return 0;
}

}  // namespace mds
}  // namespace curve

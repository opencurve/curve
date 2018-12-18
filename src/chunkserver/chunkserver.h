/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 * 
 * History: 
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "src/common/configuration.h"

#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/qos_manager.h"
#include "src/chunkserver/integrity.h"
#include "src/chunkserver/service_manager.h"
#include "proto/chunkserver.pb.h"

#ifndef SRC_CHUNKSERVER_CHUNKSERVER_H_
#define SRC_CHUNKSERVER_CHUNKSERVER_H_

namespace curve {
namespace chunkserver {

class ChunkServer {
 public:
    ChunkServer() {}
    ~ChunkServer() {}

    int Init(int argc, char** argv);
    int Fini();
    int Run();
    int Stop();

    CHUNKSERVER_STATE GetState();
    ChunkServerInfo GetInfo();

 private:
    int ParseCommandLineFlags(int argc, char** argv);
    int ReplaceConfigWithCmdFlags();
    int RegisterToMetaServer();
    int PingMetaServer();

    void InitCopysetNodeOptions();
    void InitQosOptions();
    void InitIntegrityOptions();
    void InitServiceOptions();

    common::Configuration       conf_;

    CopysetNodeOptions          copysetNodeOptions_;
    CopysetNodeManager          copysetNodeManager_;

    QosOptions                  qosOptions_;
    QosManager                  qosManager_;

    IntegrityOptions            integrityOptions_;
    Integrity                   integrity_;

    ServiceOptions              serviceOptions_;
    ServiceManager              serviceManager_;

    // TODO(wenyu): to be implement
    // StoreEngineAdaptor       storeng;
    // Monitor                  monitor;

    bool                        toStop;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_CHUNKSERVER_H_

/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_METASERVER_METASERVER_H_
#define CURVEFS_SRC_METASERVER_METASERVER_H_

#include <brpc/server.h>

#include <memory>
#include <string>

#include "curvefs/src/metaserver/copyset/config.h"
#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "curvefs/src/metaserver/copyset/raft_cli_service2.h"
#include "curvefs/src/metaserver/copyset/copyset_service.h"
#include "curvefs/src/metaserver/heartbeat.h"
#include "curvefs/src/metaserver/inflight_throttle.h"
#include "curvefs/src/metaserver/metaserver_service.h"
#include "curvefs/src/metaserver/partition_clean_manager.h"
#include "curvefs/src/client/rpcclient/base_client.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/rpcclient/metaserver_client.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/register.h"
#include "src/common/configuration.h"
#include "src/fs/local_filesystem.h"
#include "curvefs/src/metaserver/resource_statistic.h"
#include "curvefs/src/metaserver/recycle_manager.h"

namespace curvefs {
namespace metaserver {

using ::curve::common::Configuration;
using ::curvefs::metaserver::storage::StorageOptions;
using ::curvefs::metaserver::copyset::CopysetNodeManager;
using ::curvefs::metaserver::copyset::CopysetNodeOptions;
using ::curvefs::metaserver::copyset::CopysetServiceImpl;
using ::curvefs::metaserver::copyset::RaftCliService2;
using ::curvefs::client::rpcclient::MdsClient;
using ::curvefs::client::rpcclient::MdsClientImpl;
using ::curvefs::client::rpcclient::MDSBaseClient;
using ::curvefs::client::rpcclient::MetaServerClient;
using ::curvefs::client::common::MdsOption;
using ::curvefs::metaserver::storage::StorageOptions;

struct MetaserverOptions {
    std::string ip;
    int port;
    std::string externalIp;
    int externalPort;
    int bthreadWorkerCount = -1;
    int idleTimeoutSec = -1;
    bool enableExternalServer;
};

class Metaserver {
 public:
    void InitOptions(std::shared_ptr<Configuration> conf);
    void Init();
    void Run();
    void Stop();

 private:
    void InitStorage();
    void InitCopysetNodeOptions();
    void InitCopysetNodeManager();
    void InitLocalFileSystem();
    void InitInflightThrottle();
    void InitHeartbeatOptions();
    void InitResourceCollector();
    void InitHeartbeat();
    void InitMetaClient();
    void InitRegisterOptions();
    void InitBRaftFlags(const std::shared_ptr<Configuration>& conf);
    void InitPartitionOption(std::shared_ptr<S3ClientAdaptor> s3Adaptor,
                              std::shared_ptr<MdsClient> mdsClient,
                             PartitionCleanOption* partitionCleanOption);
    void InitRecycleManagerOption(
                RecycleManagerOption* recycleManagerOption);
    void GetMetaserverDataByLoadOrRegister();
    int PersistMetaserverMeta(std::string path, MetaServerMetadata* metadata);
    int LoadMetaserverMeta(const std::string& metaFilePath,
                           MetaServerMetadata* metadata);
    int LoadDataFromLocalFile(std::shared_ptr<LocalFileSystem> fs,
                              const std::string& localPath, std::string* data);
    int PersistDataToLocalFile(std::shared_ptr<LocalFileSystem> fs,
                               const std::string& localPath,
                               const std::string& data);

 private:
    // metaserver configuration items
    std::shared_ptr<Configuration> conf_;
    // initialized or not
    bool inited_ = false;
    // running as the main MDS or not
    bool running_ = false;

    std::shared_ptr<S3ClientAdaptor>  s3Adaptor_;
    std::shared_ptr<MdsClient> mdsClient_;
    std::shared_ptr<MetaServerClient> metaClient_;
    MDSBaseClient* mdsBase_;
    MdsOption mdsOptions_;
    MetaserverOptions options_;
    MetaServerMetadata metadata_;
    std::string metaFilePath_;

    std::unique_ptr<brpc::Server> server_;
    std::unique_ptr<brpc::Server> externalServer_;

    std::unique_ptr<MetaServerServiceImpl> metaService_;
    std::unique_ptr<CopysetServiceImpl> copysetService_;
    std::unique_ptr<RaftCliService2> raftCliService2_;

    HeartbeatOptions heartbeatOptions_;
    Heartbeat heartbeat_;

    std::unique_ptr<ResourceCollector> resourceCollector_;

    CopysetNodeOptions copysetNodeOptions_;
    CopysetNodeManager* copysetNodeManager_;

    RegisterOptions registerOptions_;

    std::unique_ptr<InflightThrottle> inflightThrottle_;
    std::shared_ptr<curve::fs::LocalFileSystem> localFileSystem_;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_METASERVER_H_

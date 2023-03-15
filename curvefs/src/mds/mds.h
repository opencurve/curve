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

#ifndef CURVEFS_SRC_MDS_MDS_H_
#define CURVEFS_SRC_MDS_MDS_H_

#include <bvar/bvar.h>

#include <memory>
#include <string>

#include "curvefs/proto/heartbeat.pb.h"
#include "curvefs/src/mds/chunkid_allocator.h"
#include "curvefs/src/mds/fs_manager.h"
#include "curvefs/src/mds/heartbeat/heartbeat_service.h"
#include "curvefs/src/mds/schedule/coordinator.h"
#include "curvefs/src/mds/topology/topology.h"
#include "curvefs/src/mds/topology/topology_config.h"
#include "curvefs/src/mds/topology/topology_metric.h"
#include "curvefs/src/mds/topology/topology_service.h"
#include "curvefs/src/mds/topology/topology_storge_etcd.h"
#include "curvefs/src/mds/space/manager.h"
#include "curvefs/src/mds/dlock/dlock.h"
#include "src/common/configuration.h"
#include "src/kvstorageclient/etcd_client.h"
#include "src/leader_election/leader_election.h"
#include "src/common/s3_adapter.h"
#include "curvefs/src/mds/space/service.h"
#include "curvefs/src/mds/space/mds_proxy_options.h"

using ::curve::common::Configuration;
using ::curvefs::mds::topology::TopologyOption;
using ::curvefs::mds::topology::TopologyImpl;
using ::curvefs::mds::topology::TopologyManager;
using ::curvefs::mds::topology::DefaultIdGenerator;
using ::curvefs::mds::topology::DefaultTokenGenerator;
using ::curvefs::mds::topology::TopologyStorageEtcd;
using ::curvefs::mds::topology::TopologyStorageCodec;
using ::curvefs::mds::topology::TopologyServiceImpl;
using ::curvefs::mds::topology::TopologyMetricService;
using ::curvefs::mds::heartbeat::HeartbeatServiceImpl;
using ::curvefs::mds::heartbeat::HeartbeatOption;
using ::curve::kvstorage::EtcdClientImp;
using ::curvefs::mds::schedule::Coordinator;
using ::curvefs::mds::schedule::ScheduleOption;
using ::curvefs::mds::schedule::ScheduleMetrics;
using ::curvefs::mds::schedule::TopoAdapterImpl;
using ::curve::common::S3Adapter;

namespace curvefs {
namespace mds {

using ::curve::common::Configuration;
using ::curve::election::LeaderElection;
using ::curve::election::LeaderElectionOptions;
using curve::kvstorage::EtcdClientImp;
using ::curve::kvstorage::KVStorageClient;

// TODO(split InitEtcdConf): split this InitEtcdConf to a single module

using ::curvefs::mds::space::SpaceManager;
using ::curvefs::mds::dlock::DLockOptions;

using ::curvefs::mds::space::MdsProxyOptions;

struct MDSOptions {
    int dummyPort;
    std::string mdsListenAddr;
    MetaserverOptions metaserverOptions;
    // TODO(add EtcdConf): add etcd configure

    uint64_t mdsSpaceCalIntervalSec;

    TopologyOption topologyOptions;
    HeartbeatOption heartbeatOption;
    ScheduleOption scheduleOption;

    DLockOptions dLockOptions;

    MdsProxyOptions bsMdsProxyOptions;
};

class MDS {
 public:
    MDS();
    ~MDS();

    MDS(const MDS&) = delete;
    MDS& operator=(const MDS&) = delete;

    void InitOptions(std::shared_ptr<Configuration> conf);
    void Init();
    void Run();
    void Stop();

    // Start dummy server for metric
    void StartDummyServer();

    // Start leader election
    void StartCompaginLeader();

 private:
    void InitEtcdClient();
    void InitEtcdConf(EtcdConf* etcdConf);
    bool CheckEtcd();

    void InitLeaderElectionOption(LeaderElectionOptions* option);
    void InitLeaderElection(const LeaderElectionOptions& option);

    void InitHeartbeatOption(HeartbeatOption* heartbeatOption);
    void InitScheduleOption(ScheduleOption* scheduleOption);

    void InitDLockOptions(DLockOptions* dLockOptions);

 private:
    void InitMetaServerOption(MetaserverOptions* metaserverOption);
    void InitTopologyOption(TopologyOption* topologyOption);

    void InitTopology(const TopologyOption& option);

    void InitTopologyManager(const TopologyOption& option);

    void InitTopologyMetricService(const TopologyOption& option);

    void InitHeartbeatManager();

    void InitCoordinator();

    void InitFsManagerOptions(FsManagerOption* fsManagerOption);

    void InitMdsProxyManagerOptions(MdsProxyOptions* options);

 private:
    // mds configuration items
    std::shared_ptr<Configuration> conf_;
    // initialized or not
    bool inited_;
    // running as the main MDS or not
    bool running_;
    std::shared_ptr<FsManager> fsManager_;
    std::shared_ptr<FsStorage> fsStorage_;
    std::shared_ptr<SpaceManager> spaceManager_;
    std::shared_ptr<MetaserverClient> metaserverClient_;
    std::shared_ptr<ChunkIdAllocator> chunkIdAllocator_;
    std::shared_ptr<TopologyImpl> topology_;
    std::shared_ptr<TopologyManager> topologyManager_;
    std::shared_ptr<Coordinator> coordinator_;
    std::shared_ptr<HeartbeatManager> heartbeatManager_;
    std::shared_ptr<TopologyMetricService> topologyMetricService_;
    std::shared_ptr<S3Adapter> s3Adapter_;
    MDSOptions options_;

    bool etcdClientInited_;
    std::shared_ptr<curve::kvstorage::EtcdClientImp> etcdClient_;

    std::shared_ptr<curve::election::LeaderElection> leaderElection_;

    std::shared_ptr<curve::idgenerator::EtcdIdGenerator> idGen_;

    bvar::Status<std::string> status_;

    std::string etcdEndpoint_;
};

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_MDS_H_

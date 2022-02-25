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
#include "src/common/configuration.h"
#include "src/kvstorageclient/etcd_client.h"
#include "src/leader_election/leader_election.h"
#include "curvefs/src/mds/s3/mds_s3.h"

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
using curvefs::mds::S3Client;

namespace curvefs {
namespace mds {

using ::curve::common::Configuration;
using ::curve::election::LeaderElection;
using ::curve::election::LeaderElectionOptions;
using curve::kvstorage::EtcdClientImp;
using ::curve::kvstorage::KVStorageClient;
// TODO(split InitEtcdConf): split this InitEtcdConf to a single module

struct MDSOptions {
    int dummyPort;
    std::string mdsListenAddr;
    SpaceOptions spaceOptions;
    MetaserverOptions metaserverOptions;
    // TODO(add EtcdConf): add etcd configure

    TopologyOption topologyOptions;
    HeartbeatOption heartbeatOption;
    ScheduleOption scheduleOption;
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

 private:
    void InitSpaceOption(SpaceOptions* spaceOption);
    void InitMetaServerOption(MetaserverOptions* metaserverOption);
    void InitTopologyOption(TopologyOption* topologyOption);

    void InitTopology(const TopologyOption& option);

    void InitTopologyManager(const TopologyOption& option);

    void InitTopologyMetricService(const TopologyOption& option);

    void InitHeartbeatManager();

    void InitCoordinator();

    void InitFsManagerOptions(FsManagerOption* fsManagerOption);

 private:
    // mds configuration items
    std::shared_ptr<Configuration> conf_;
    // initialized or not
    bool inited_;
    // running as the main MDS or not
    bool running_;
    std::shared_ptr<FsManager> fsManager_;
    std::shared_ptr<FsStorage> fsStorage_;
    std::shared_ptr<SpaceClient> spaceClient_;
    std::shared_ptr<MetaserverClient> metaserverClient_;
    std::shared_ptr<ChunkIdAllocator> chunkIdAllocator_;
    std::shared_ptr<TopologyImpl> topology_;
    std::shared_ptr<TopologyManager> topologyManager_;
    std::shared_ptr<Coordinator> coordinator_;
    std::shared_ptr<HeartbeatManager> heartbeatManager_;
    std::shared_ptr<TopologyMetricService> topologyMetricService_;
    std::shared_ptr<S3Client> s3Client_;
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

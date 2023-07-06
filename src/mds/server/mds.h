/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 2020-01-03
 * Author: charisu
 */
#ifndef SRC_MDS_SERVER_MDS_H_
#define SRC_MDS_SERVER_MDS_H_

#include <glog/logging.h>
#include <gflags/gflags.h>

#include <brpc/channel.h>
#include <brpc/server.h>
#include <string>
#include <memory>
#include <map>

#include "src/common/authenticator.h"
#include "src/mds/nameserver2/namespace_storage.h"
#include "src/mds/nameserver2/namespace_service.h"
#include "src/mds/nameserver2/curvefs.h"
#include "src/mds/nameserver2/clean_manager.h"
#include "src/mds/nameserver2/clean_core.h"
#include "src/mds/nameserver2/clean_task_manager.h"
#include "src/mds/nameserver2/chunk_allocator.h"
#include "src/leader_election/leader_election.h"
#include "src/mds/topology/topology_chunk_allocator.h"
#include "src/mds/topology/topology_service.h"
#include "src/mds/topology/topology_id_generator.h"
#include "src/mds/topology/topology_token_generator.h"
#include "src/mds/topology/topology_config.h"
#include "src/mds/topology/topology_stat.h"
#include "src/mds/topology/topology_metric.h"
#include "src/mds/schedule/scheduleMetrics.h"
#include "src/mds/copyset/copyset_manager.h"
#include "src/common/configuration.h"
#include "src/mds/heartbeat/heartbeat_service.h"
#include "src/mds/schedule/topoAdapter.h"
#include "proto/heartbeat.pb.h"
#include "src/mds/chunkserverclient/chunkserverclient_config.h"
#include "src/mds/nameserver2/allocstatistic/alloc_statistic.h"
#include "src/common/curve_version.h"
#include "src/common/channel_pool.h"
#include "src/mds/schedule/scheduleService/scheduleService.h"
#include "src/common/concurrent/dlock.h"
#include "src/mds/auth/auth_service.h"

using ::curve::mds::topology::TopologyChunkAllocatorImpl;
using ::curve::mds::topology::TopologyServiceImpl;
using ::curve::mds::topology::DefaultIdGenerator;
using ::curve::mds::topology::DefaultTokenGenerator;
using ::curve::mds::topology::TopologyImpl;
using ::curve::mds::topology::TopologyOption;
using ::curve::mds::topology::TopologyStatImpl;
using ::curve::mds::topology::ChunkFilePoolAllocHelp;
using ::curve::mds::topology::TopologyMetricService;
using ::curve::mds::topology::TopologyServiceManager;
using ::curve::mds::copyset::CopysetManager;
using ::curve::mds::copyset::CopysetOption;
using ::curve::mds::heartbeat::HeartbeatServiceImpl;
using ::curve::mds::heartbeat::HeartbeatOption;
using ::curve::mds::schedule::TopoAdapterImpl;
using ::curve::mds::schedule::TopoAdapter;
using ::curve::mds::schedule::ScheduleOption;
using ::curve::mds::schedule::ScheduleMetrics;
using ::curve::mds::schedule::ScheduleServiceImpl;
using ::curve::mds::chunkserverclient::ChunkServerClientOption;
using ::curve::mds::snapshotcloneclient::SnapshotCloneClientOption;
using ::curve::election::LeaderElectionOptions;
using ::curve::election::LeaderElection;
using ::curve::common::Configuration;
using ::curve::common::DLockOpts;
using ::curve::mds::auth::AuthOption;
using ::curve::common::ServerAuthOption;
using ::curve::mds::auth::AuthServiceManager;
using ::curve::mds::auth::AuthNodeImpl;
using ::curve::mds::auth::AuthServiceImpl;
using ::curve::mds::auth::AuthStorageEtcd;
using ::curve::mds::auth::AuthStorage;

namespace curve {
namespace mds {

struct MDSOptions {
    // dummyserver port
    int dummyListenPort;
    // the address that the master mds listen to
    std::string mdsListenAddr;
    // configuration of segmentAlloc
    uint64_t retryInterTimes;
    uint64_t periodicPersistInterMs;
    // cache size of namestorage
    int mdsCacheCount;
    int mdsFilelockBucketNum;

    FileRecordOptions fileRecordOptions;
    RootAuthOption rootAuthOptions;
    CurveFSOption curveFSOptions;
    ScheduleOption scheduleOption;
    HeartbeatOption heartbeatOption;
    TopologyOption topologyOption;
    CopysetOption copysetOption;
    ChunkServerClientOption chunkServerClientOption;
    SnapshotCloneClientOption snapshotCloneClientOption;
    // for auth service
    AuthOption authOption;
    // for mds authentication
    ServerAuthOption serverAuthOption;
    // for auth client
    curve::common::AuthClientOption authClientOption;
};

class MDS {
 public:
    MDS() : inited_(false), running_(false) {}

    ~MDS();

    /**
     * @brief initialize mds options from configuration files
     */
    void InitMdsOptions(std::shared_ptr<Configuration> conf);

    /**
     * @brief start MDS DummyServer for liveness probe for all mds
     *        and get metrics like version and configuration
     */
    void StartDummy();

    /**
     * @brief start leader election
     *
     */
    void StartCompaginLeader();

    /**
     * @brief components initialization
     */
    void Init();

    /**
     * @brief run mds
     */
    void Run();

    /**
     * @brief stop mds
     */
    void Stop();

 private:
    void InitFileRecordOptions(FileRecordOptions *fileRecordOptions);

    void InitRootAuthOptions(RootAuthOption *authOptions);

    void InitCurveFSOptions(CurveFSOption *curveFSOptions);

    void InitScheduleOption(ScheduleOption *scheduleOption);

    void InitHeartbeatOption(HeartbeatOption* heartbeatOption);

    void InitEtcdConf(EtcdConf* etcdConf);

    void InitMdsLeaderElectionOption(LeaderElectionOptions* electionOp);

    void InitTopologyOption(TopologyOption *topologyOption);

    void InitCopysetOption(CopysetOption *copysetOption);

    void InitChunkServerClientOption(ChunkServerClientOption *option);

    void InitSnapshotCloneClientOption(SnapshotCloneClientOption *option);

    void InitEtcdClient(const EtcdConf& etcdConf,
                        int etcdTimeout,
                        int retryTimes);

    void InitLeaderElection(const LeaderElectionOptions& leaderElectionOp);

    void InitSegmentAllocStatistic(uint64_t retryInterTimes,
                                   uint64_t periodicPersistInterMs);

    void InitNameServerStorage(int mdsCacheCount);

    void StartServer();

    void InitTopologyModule();

    void InitTopology(const TopologyOption& option);

    void InitTopologyStat();

    void InitTopologyMetricService(const TopologyOption& option);

    void InitTopologyServiceManager(const TopologyOption& option);

    void InitTopologyChunkAllocator(const TopologyOption& option);

    void InitCurveFS(const CurveFSOption& curveFSOptions);

    void InitCleanManager();

    void InitCoordinator();

    void InitHeartbeatManager();

    void InitSnapshotCloneClient();

    void InitThrottleOption(ThrottleOption* option);

    void InitDLockOption(std::shared_ptr<DLockOpts> dlockOpts);

    void InitAuthManagerOption(AuthOption *option);

    void InitMdsAuthOption(ServerAuthOption *option);

    void InitAuthClientOption(curve::common::AuthClientOption *option);

    void InitAuthServiceManager(const AuthOption &option,
      const curve::common::AuthClientOption &authClientOption);

 private:
    // mds configuration items
    std::shared_ptr<Configuration> conf_;
    // initialized or not
    bool inited_;
    // running as the main MDS or not
    bool running_;
    // mds status, leader or follower
    bvar::Status<std::string> status_;
    MDSOptions options_;

    std::shared_ptr<EtcdClientImp> etcdClient_;
    std::shared_ptr<LeaderElection> leaderElection_;
    std::shared_ptr<AllocStatistic> segmentAllocStatistic_;
    std::shared_ptr<NameServerStorage> nameServerStorage_;
    std::shared_ptr<TopologyImpl> topology_;
    std::shared_ptr<TopologyStatImpl> topologyStat_;
    std::shared_ptr<TopologyChunkAllocator> topologyChunkAllocator_;
    std::shared_ptr<TopologyMetricService> topologyMetricService_;
    std::shared_ptr<TopologyServiceManager> topologyServiceManager_;
    std::shared_ptr<CleanManager> cleanManager_;
    std::shared_ptr<CleanDiscardSegmentTask> cleanDiscardSegmentTask_;
    std::shared_ptr<Coordinator> coordinator_;
    std::shared_ptr<HeartbeatManager> heartbeatManager_;
    char* etcdEndpoints_;
    FileLockManager* fileLockManager_;
    std::shared_ptr<SnapshotCloneClient> snapshotCloneClient_;
    std::shared_ptr<AuthServiceManager> authServiceManager_;
    std::shared_ptr<Authenticator> authenticator_;
};

bool ParsePoolsetRules(const std::string& str,
                       std::map<std::string, std::string>* rules);

bool CheckOrInsertBlockSize(EtcdClientImp* etcdclient);
bool CheckOrInsertChunkSize(EtcdClientImp* etcdclient);

}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SERVER_MDS_H_

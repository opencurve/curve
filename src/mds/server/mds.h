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

using ::curve::mds::topology::TopologyChunkAllocatorImpl;
using ::curve::mds::topology::TopologyServiceImpl;
using ::curve::mds::topology::DefaultIdGenerator;
using ::curve::mds::topology::DefaultTokenGenerator;
using ::curve::mds::topology::TopologyImpl;
using ::curve::mds::topology::TopologyOption;
using ::curve::mds::topology::TopologyStatImpl;
using ::curve::mds::topology::TopologyMetricService;
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
using ::curve::election::LeaderElectionOptions;
using ::curve::election::LeaderElection;
using ::curve::common::Configuration;

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
    // bucket size of mds file lock
    int mdsFilelockBucketNum;

    FileRecordOptions fileRecordOptions;
    RootAuthOption authOptions;
    CurveFSOption curveFSOptions;
    ScheduleOption scheduleOption;
    HeartbeatOption heartbeatOption;
    TopologyOption topologyOption;
    CopysetOption copysetOption;
    ChunkServerClientOption chunkServerClientOption;
};

class MDS {
 public:
    MDS() : inited_(false), running_(false) {}

    ~MDS();

    /**
     * @brief initialize mds options (read) from configuration files
     */
    void InitMdsOptions(std::shared_ptr<Configuration> conf);

    /**
     * @brief start MDS DummyServer for liveness probe and flags fetching 
     *        between master and slave MDS servers
     *        (for exposing program version, fetching configuration from files etc.)
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
    /**
     * @brief initialize session options
     * @param fileRecordOptions session related options
     */
    void InitFileRecordOptions(FileRecordOptions *fileRecordOptions);

    /**
     * @brief initialize authentication options
     * @param authOptions authentication options
     */
    void InitAuthOptions(RootAuthOption *authOptions);

    /**
     * @brief initialize curveFS options
     * @param curveFSOptions curveFS options
     */
    void InitCurveFSOptions(CurveFSOption *curveFSOptions);

    /**
     * @brief initialize scheduling options
     * @param[out] scheduleOption scheduling options
     */
    void InitScheduleOption(ScheduleOption *scheduleOption);

    /**
     * @brief initialize heartbeat options
     * @param[out] heartbeatOption heartbeat options
     */
    void InitHeartbeatOption(HeartbeatOption* heartbeatOption);

    /**
     * @brief initialize etcd configurations
     * @param[out] etcdConf etcd configurations
     */
    void InitEtcdConf(EtcdConf* etcdConf);

    /**
     * @brief initialize leader election options
     * @[out] electionOp leader election options
     */
    void InitMdsLeaderElectionOption(LeaderElectionOptions* electionOp);

    /**
     * @brief initialize topology options
     * @param[out] topologyOption topology options
     */
    void InitTopologyOption(TopologyOption *topologyOption);

    /**
     * @brief initialize copyset options
     * @param[out] copysetOption copyset options
     */
    void InitCopysetOption(CopysetOption *copysetOption);

    /**
     * @brief initialize chunkserver client option
     * @param[out] option chunkserver client option
     */
    void InitChunkServerClientOption(ChunkServerClientOption *option);

    /**
     * @brief initialize etcd client
     * @param etcdConf etcd configuration
     * @param etcdTimeout timeout peroid
     * @param retryTimes retry times
     */
    void InitEtcdClient(const EtcdConf& etcdConf,
                        int etcdTimeout,
                        int retryTimes);

    /**
     * @brief initialize leader election module
     * @param leaderElectionOp leader election options
     */
    void InitLeaderElection(const LeaderElectionOptions& leaderElectionOp);

    /**
     * @brief initialize segment allocation and statistic module
     * @param retryInterTimes retry interval
     * @param periodicPersistInterMs time interval of RAM data persistance (in ms)
     */
    void InitSegmentAllocStatistic(uint64_t retryInterTimes,
                                   uint64_t periodicPersistInterMs);

    /**
     * @brief initialize nameserver storage module
     * @param mdsCacheCount 缓存大小
     */
    void InitNameServerStorage(int mdsCacheCount);

    /**
     * @brief run brpc module
     */
    void StartServer();

    /**
     * @brief 初始化topology相关模块
     */
    void InitTopologyModule();

    /**
     * @brief 初始化topology模块
     * @param option topology相关选项
     */
    void InitTopology(const TopologyOption& option);

    /**
     * @brief 初始化topology统计模块
     */
    void InitTopologyStat();

    /**
     * @brief 初始化topology metric模块
     * @param option topology相关选项
     */
    void InitTopologyMetricService(const TopologyOption& option);

    /**
     * @brief 初始化topology service管理模块
     * @param option topology相关选项
     */
    void InitTopologyServiceManager(const TopologyOption& option);

    /**
     * @brief 初始化chunk分配模块
     * @param option topology相关选项
     */
    void InitTopologyChunkAllocator(const TopologyOption& option);

    /**
     * @brief 初始化curveFS
     */
    void InitCurveFS(const CurveFSOption& curveFSOptions);

    /**
     * @brief 初始化异步清理模块
     */
    void InitCleanManager();

    /**
     * @brief 初始化调度模块
     */
    void InitCoordinator();

    /**
     * @brief 初始化心跳模块
     */
    void InitHeartbeatManager();

 private:
    // mds配置项
    std::shared_ptr<Configuration> conf_;
    // 是否初始化过
    bool inited_;
    // 是否作为主MDS在运行
    bool running_;
    // mds状态，leader或follower
    bvar::Status<std::string> status_;
    // mds相关选项
    MDSOptions options_;

    // 与etcd交互的client
    std::shared_ptr<EtcdClientImp> etcdClient_;
    // leader竞选模块
    std::shared_ptr<LeaderElection> leaderElection_;
    // segment分配统计模块
    std::shared_ptr<AllocStatistic> segmentAllocStatistic_;
    // NameServer存储模块
    std::shared_ptr<NameServerStorage> nameServerStorage_;
    // topology模块，用于定期把内存中的topology数据持久化
    std::shared_ptr<TopologyImpl> topology_;
    // topology统计模块
    std::shared_ptr<TopologyStatImpl> topologyStat_;
    // chunk分配模块
    std::shared_ptr<TopologyChunkAllocator> topologyChunkAllocator_;
    // topology metric
    std::shared_ptr<TopologyMetricService> topologyMetricService_;
    // topology service管理模块
    std::shared_ptr<TopologyServiceManager> topologyServiceManager_;
    // 异步清理模块
    std::shared_ptr<CleanManager> cleanManager_;
    // 调度模块
    std::shared_ptr<Coordinator> coordinator_;
    // 心跳模块
    std::shared_ptr<HeartbeatManager> heartbeatManager_;
    // etcd节点信息，这个是go里面的逻辑，没法用智能指针
    char* etcdEndpoints_;
    // 文件锁管理对象
    FileLockManager* fileLockManager_;
};

}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SERVER_MDS_H_

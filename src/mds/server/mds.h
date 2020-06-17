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
    // 主mds对外核心服务监听地址
    std::string mdsListenAddr;
    // segmentAlloc相关配置
    uint64_t retryInterTimes;
    uint64_t periodicPersistInterMs;
    // namestorage的缓存大小
    int mdsCacheCount;
    // mds的文件锁桶大小
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
     * @brief 从配置初始化mds相关option
     */
    void InitMdsOptions(std::shared_ptr<Configuration> conf);

    /**
     * @brief 启动MDS DummyServer 用于主从MDS的基础探活和flags获取
     *        比如用于暴露程序版本、从配置文件中获取的所有配置等等
     */
    void StartDummy();

    /**
     * @brief 开始竞选leader
     *
     */
    void StartCompaginLeader();

    /**
     * @brief 初始化各个组件
     */
    void Init();

    /**
     * @brief 启动mds
     */
    void Run();

    /**
     * @brief 停止mds
     */
    void Stop();

 private:
    /**
     * @brief 初始化session相关选项
     * @param session相关选项
     */
    void InitFileRecordOptions(FileRecordOptions *fileRecordOptions);

    /**
     * @brief 初始化认证选项
     * @param 认证相关选项
     */
    void InitAuthOptions(RootAuthOption *authOptions);

    /**
     * @brief 初始化curveFs选项
     * @param curveFs选项
     */
    void InitCurveFSOptions(CurveFSOption *curveFSOptions);

    /**
     * @brief 初始化调度相关选项
     * @param[out] scheduleOption 调度相关选项
     */
    void InitScheduleOption(ScheduleOption *scheduleOption);

    /**
     * @brief 初始化心跳相关选项
     * param[out] 心跳相关选项
     */
    void InitHeartbeatOption(HeartbeatOption* heartbeatOption);

    /**
     * @brief 初始化etcd conf
     * @param[out] etcdConf etcd的配置信息
     */
    void InitEtcdConf(EtcdConf* etcdConf);

    /**
     * @brief 初始化leader选举option
     * @[out] electionOp leader选举选项
     */
    void InitMdsLeaderElectionOption(LeaderElectionOptions* electionOp);

    /**
     * @brief 初始化topology option
     * @param[out] topologyOption topology相关选项
     */
    void InitTopologyOption(TopologyOption *topologyOption);

    /**
     * @brief 初始化copyset选项
     * @param[out] copyset 选项
     */
    void InitCopysetOption(CopysetOption *copysetOption);

    /**
     * @brief 初始化chunkserver client option
     * @param[out] option chunkserverClient相关选项
     */
    void InitChunkServerClientOption(ChunkServerClientOption *option);

    /**
     * @brief 初始化etcd client
     * @param etcdConf etcd配置项
     * @param etcdTimeout 超时时间
     * @param retryTimes 重试次数
     */
    void InitEtcdClient(const EtcdConf& etcdConf,
                        int etcdTimeout,
                        int retryTimes);

    /**
     * @brief 初始化leader选举模块
     * @param leaderElectionOp leader选举选项
     */
    void InitLeaderElection(const LeaderElectionOptions& leaderElectionOp);

    /**
     * @brief 初始化segment分配统计模块
     * @param retryInterTimes 重试间隔
     * @param periodicPersistInterMs 将内存中的数据持久化到etcd的间隔, 单位ms
     */
    void InitSegmentAllocStatistic(uint64_t retryInterTimes,
                                   uint64_t periodicPersistInterMs);

    /**
     * @brief 初始化nameserver存储模块
     * @param mdsCacheCount 缓存大小
     */
    void InitNameServerStorage(int mdsCacheCount);

    /**
     * @brief 开启brpc server
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

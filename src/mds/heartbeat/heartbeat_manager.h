/*
 * Project: curve
 * Created Date: Mon Nov 19 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_HEARTBEAT_HEARTBEAT_MANAGER_H_
#define SRC_MDS_HEARTBEAT_HEARTBEAT_MANAGER_H_

#include <vector>
#include <map>
#include <atomic>
#include <string>
#include <memory>

#include "src/mds/topology/topology.h"
#include "src/mds/common/mds_define.h"
#include "src/mds/heartbeat/topo_updater.h"
#include "src/mds/heartbeat/copyset_conf_generator.h"
#include "src/mds/heartbeat/chunkserver_healthy_checker.h"
#include "src/mds/schedule/coordinator.h"
#include "src/common/concurrent/concurrent.h"
#include "proto/heartbeat.pb.h"

using ::curve::mds::topology::CopySetInfo;
using ::curve::mds::topology::PoolIdType;
using ::curve::mds::topology::CopySetIdType;
using ::curve::mds::topology::Topology;
using ::curve::mds::schedule::Coordinator;

using ::curve::common::Thread;
using ::curve::common::Atomic;
using ::curve::common::RWLock;

namespace curve {
namespace mds {
namespace heartbeat {
// HeartbeatManager: 主要处三种类型的任务
// 1. 后台检查线程。
//    - 更新chunkserver最近一次心跳时间
//    - 定时检查chunkserver的在线状态
// 3. 下发copyset的配置信息。
//    - mds不存在上报的copyset，下发空配置指导chunkserver清理该copyset数据
//    - 将copyset的信息pass到scheduler模块，check是否有配置变更需要下发
//    - follower copyset的配置不包含chunkserver, 下发空配置指导chunkserver清理
// 2. 更新topology信息。
//    - 根据chunkserver上报的copyset的信息，更新topology中copyset的epoch,
//      副本关系, 统计信息等
class HeartbeatManager {
 public:
    HeartbeatManager(HeartbeatOption option,
        std::shared_ptr<Topology> topology,
        std::shared_ptr<Coordinator> coordinator);

    ~HeartbeatManager() {}

    /*
    * @brief Init 用于mds初始化心跳模块, 把所有chunkserver注册到chunkserver健康
    *             检查模块(class ChunkserverHealthyChecker)，chunkserver初始均设为
    *             online状态
    */
    void Init();

    /*
    * @brief Run 起一个子线程运行健康检查模块，定时检查chunkserver的心跳是否miss
    */
    void Run();

    /*
    * @brief Stop 停止心跳后端线程
    */
    void Stop();

    /**
     * @brief ChunkServerHeartbeat处理心跳请求
     *
     * @param[in] request 心跳rpc请求
     * @param[out] response 心跳处理结果
     */
    void ChunkServerHeartbeat(const ChunkServerHeartbeatRequest &request,
                                ChunkServerHeartbeatResponse *response);

 private:
    /**
     * @brief ChunkServerHealthyChecker 心跳超时检查后端线程
     */
    void ChunkServerHealthyChecker();

    /**
     * @brief CheckRequest 检查心跳上报的request内容是否合法
     *
     * @return 合法返回true, 有非法参数返回false
     */
    bool CheckRequest(const ChunkServerHeartbeatRequest &request);

    // TODO(lixiaocui): 优化，统一heartbeat和topology中两个CopySetInfo的名字
    /**
     * @brief FromHeartbeatCopySetInfoToTopologyOne 把心跳中copyset struct转化成
     *        topology中copyset struct
     *
     * @param[in] info 心跳上报的copyset信息
     * @param[out] out topology中的心跳结构
     *
     * @return 转化成功为true, 失败为false
     */
    bool FromHeartbeatCopySetInfoToTopologyOne(
        const ::curve::mds::heartbeat::CopySetInfo &info,
        ::curve::mds::topology::CopySetInfo *out);

    /**
     * @brief GetChunkserverIdByPeerStr 心跳上报上来的chunkserver是ip:port:id的形式，
     *       该函数从string中解析出ip,port,并根据ip,port从topo中获取对应的chunkserverId
     *
     * @param[in] peer ip:port:id形式的chunkserver信息
     *
     * @return 根据ip:port获取的chunkserverId
     */
    ChunkServerIdType GetChunkserverIdByPeerStr(std::string peer);

 private:
    // heartbeat相关依赖
    std::shared_ptr<Topology> topology_;
    std::shared_ptr<Coordinator> coordinator_;

    // healthyChecker_ 后台线程checker逻辑
    std::shared_ptr<ChunkserverHealthyChecker> healthyChecker_;
    // topoUpdater_ 更新topo中copyset的epoch, 复制组关系等信息
    std::shared_ptr<TopoUpdater> topoUpdater_;
    // CopysetConfGenerator 根据新旧copyset的信息确认是否需要生成命令下发给chunkserver //NOLINT
    // 处理如下几种情况:
    // 1. chunkserver上报的copyset在mds中不存在
    // 2. leader copyset
    // 3. copyset的最新复制组中不包含该chunkserver
    std::shared_ptr<CopysetConfGenerator> copysetConfGenerator_;

    // 管理chunkserverHealthyChecker线程
    Thread backEndThread_;
    Atomic<bool> isStop_;
    int chunkserverHealthyCheckerRunInter_;
};

}  // namespace heartbeat
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_HEARTBEAT_HEARTBEAT_MANAGER_H_

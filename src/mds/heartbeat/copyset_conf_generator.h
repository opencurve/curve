/*
 * Project: curve
 * Created Date: Mon Mar 25 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_HEARTBEAT_COPYSET_CONF_GENERATOR_H_
#define SRC_MDS_HEARTBEAT_COPYSET_CONF_GENERATOR_H_

#include <string>
#include "src/mds/topology/topology.h"
#include "src/mds/topology/topology_item.h"
#include "src/mds/schedule/coordinator.h"
#include "proto/heartbeat.pb.h"

using ::curve::mds::topology::CopySetInfo;
using ::curve::mds::topology::ChunkServer;
using ::curve::mds::topology::Topology;
using ::curve::mds::schedule::Coordinator;

namespace curve {
namespace mds {
namespace heartbeat {
class CopysetConfGenerator {
 public:
    CopysetConfGenerator(
        std::shared_ptr<Topology> topo,
        std::shared_ptr<Coordinator> coordinator) :
        topo_(topo), coordinator_(coordinator) {}

    ~CopysetConfGenerator() {}

    /*
    * @brief GenCopysetConf 根据上报以及记录的copyset的信息，
    *                        看是是否有新的配置下发给chunksever
    *
    * @param[in] reportId 上报心跳的chunkserverId
    * @param[in] reportCopySetInfo 心跳上报的copyset信息
    * @param[out] copysetConf 下发的配置变更命令
    *
    * @return 有配置变更命令下发为true, 没有则为false
    */
    bool GenCopysetConf(ChunkServerIdType reportId,
        const CopySetInfo &reportCopySetInfo, CopysetConf *copysetConf);

 private:
    /*
    * @brief LeaderGenCopysetConf 处理leader copyset信息，主要步骤是转发到调度模块
    *
    * @param[in] copySetInfo 上报的copyset信息
    * @param[out] copysetConf 调度模块生成的新的配置
    *
    * @return true-有新的配置下发， false-没有新配置下发
    */
    bool LeaderGenCopysetConf(
        const CopySetInfo &copySetInfo, CopysetConf *copysetConf);

    /*
    * @brief FollowerGenCopysetConf 处理follower copyset信息。比较上报的
    *       chunkserver是否在copyset的副本内, 如果不在，生成新的配置指导删除
    *
    * @param[in] reportId 上报心跳的chunkserverId
    * @param[in] reportCopySetInfo 上报的copyset信息
    * @param[in] recordCopySetInfo mds记录的copyset信息
    * @param[out] copysetConf 调度模块生成的新的配置
    *
    * @return true-有新的配置下发， false-没有新配置下发
    */
    bool FollowerGenCopysetConf(
        ChunkServerIdType reportId,
        const CopySetInfo &reportCopySetInfo,
        const CopySetInfo &recordCopySetInfo,
        CopysetConf *copysetConf);

    /*
    * @brief BuildPeerByChunkserverId 根据csId生成ip:port:id形式的string
    *
    * @param[in] csId chunkserver的id
    *
    * @return 生成的ip:port:id, 如果生成出错，则为""
    */
    std::string BuildPeerByChunkserverId(ChunkServerIdType csId);

 private:
    std::shared_ptr<Topology> topo_;
    std::shared_ptr<Coordinator> coordinator_;
};
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_HEARTBEAT_COPYSET_CONF_GENERATOR_H_

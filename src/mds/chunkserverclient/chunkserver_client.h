/*
 * Project: curve
 * Created Date: Fri Mar 08 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_CHUNKSERVERCLIENT_CHUNKSERVER_CLIENT_H_
#define SRC_MDS_CHUNKSERVERCLIENT_CHUNKSERVER_CLIENT_H_

#include <brpc/channel.h>
#include <butil/endpoint.h>

#include <memory>
#include <string>

#include "src/mds/common/mds_define.h"
#include "src/mds/topology/topology.h"
#include "proto/cli2.pb.h"
#include "proto/chunk.pb.h"
#include "src/mds/chunkserverclient/chunkserverclient_config.h"
#include "src/common/channel_pool.h"

using ::curve::mds::topology::Topology;
using ::curve::mds::topology::ChunkServerIdType;
using ::curve::common::ChannelPool;

namespace curve {
namespace mds {
namespace chunkserverclient {

class ChunkServerClient {
 public:
    ChunkServerClient(std::shared_ptr<Topology> topology,
        const ChunkServerClientOption &option,
        std::shared_ptr<ChannelPool> channelPool)
        : topology_(topology),
          rpcTimeoutMs_(option.rpcTimeoutMs),
          rpcRetryTimes_(option.rpcRetryTimes),
          rpcRetryIntervalMs_(option.rpcRetryIntervalMs),
          channelPool_(channelPool) {}

    virtual ~ChunkServerClient() {}

    /**
     * @brief  删除此次转储时产生的或者历史遗留的快照
     *         如果转储过程中没有产生快照，则修改chunk的correctedSn
     *
     * @param leaderId leader的ID
     * @param logicalPoolId 逻辑池的ID
     * @param copysetId 复制组的ID
     * @param chunkId chunk文件ID
     * @param correctedSn 快照chunk不存在时需要修正的correctedSn
     *
     * @return 错误码
     */
    virtual int DeleteChunkSnapshotOrCorrectSn(ChunkServerIdType leaderId,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t correctedSn);

    /**
     * @brief 删除非快照chunk文件
     *
     * @param leaderId leader的ID
     * @param logicalPoolId 逻辑池的ID
     * @param copysetId 复制组的ID
     * @param chunkId chunk文件ID
     * @param sn 文件版本号
     *
     * @return 错误码
     */
    virtual int DeleteChunk(ChunkServerIdType leaderId,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t sn);

    /**
     * @brief 获取leader
     * @detail
     *   向目标chunkserver发送报文查询leader
     *
     * @param csId 目标chunkserverId
     * @param logicalPoolId 逻辑池ID
     * @param copysetId 复制组的ID
     * @param[out] leader 当前leader
     *
     * @return 错误码
     */
    virtual int GetLeader(ChunkServerIdType csId,
        LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkServerIdType * leader);

 private:
    /**
     * @brief 从topology获取chunkserver的地址
     *
     * @param csId 目标chunkserver的ID
     * @param[out] csAddr chunkserver的地址，ip:port的形式
     *
     * @return 错误码
     */
    int GetChunkServerAddress(ChunkServerIdType csId,
                              std::string* csAddr);

    int GetOrInitChannel(ChunkServerIdType csId,
                         ChannelPtr* channelPtr);

    std::shared_ptr<Topology> topology_;
    uint32_t rpcTimeoutMs_;
    uint32_t rpcRetryTimes_;
    uint32_t rpcRetryIntervalMs_;
    std::shared_ptr<ChannelPool> channelPool_;
};

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve


#endif  // SRC_MDS_CHUNKSERVERCLIENT_CHUNKSERVER_CLIENT_H_

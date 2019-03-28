/*
 * Project: curve
 * Created Date: Fri Mar 08 2019
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_CHUNKSERVERCLIENT_COPYSET_CLIENT_H_
#define SRC_MDS_CHUNKSERVERCLIENT_COPYSET_CLIENT_H_

#include "src/mds/common/mds_define.h"
#include "src/mds/topology/topology_manager.h"

#include "src/mds/chunkserverclient/chunkserver_client.h"

using ::curve::mds::topology::Topology;
using ::curve::mds::topology::CopySetInfo;

namespace curve {
namespace mds {
namespace chunkserverclient {

class CopysetClient {
 public:
     /**
      * @brief 构造函数
      *
      * @param topology
      */
    explicit CopysetClient(std::shared_ptr<Topology> topo)
        : topo_(topo) {
        chunkserverClient_ =
            std::make_shared<ChunkServerClient>(topo);
    }

    void SetChunkServerClient(std::shared_ptr<ChunkServerClient> csClient) {
        chunkserverClient_ = csClient;
    }

    /**
     * @brief 删除Chunk快照文件
     *
     * @param logicPoolId 逻辑池id
     * @param copysetId 复制组id
     * @param chunkId Chunk文件id
     * @param sn 文件版本号
     *
     * @return 错误码
     */
    int DeleteSnapShotChunk(LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t sn);

    /**
     * @brief 删除非快照文件的Chunk文件
     *
     * @param logicPoolId 逻辑池id
     * @param copysetId 复制组id
     * @param chunkId Chunk文件id
     * @param sn 文件版本号
     *
     * @return 错误码
     */
    int DeleteChunk(LogicalPoolID logicalPoolId,
        CopysetID copysetId,
        ChunkID chunkId,
        uint64_t sn);

    /**
     * @brief 更新leader
     *
     * @param[int][out] copyset
     *
     * @return 错误码
     */
    int UpdateLeader(CopySetInfo *copyset);

 private:
    std::shared_ptr<Topology> topo_;
    std::shared_ptr<ChunkServerClient> chunkserverClient_;
};

}  // namespace chunkserverclient
}  // namespace mds
}  // namespace curve


#endif  // SRC_MDS_CHUNKSERVERCLIENT_COPYSET_CLIENT_H_

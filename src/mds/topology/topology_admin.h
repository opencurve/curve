/*
 * Project: curve
 * Created Date: Fri Oct 12 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_MDS_TOPOLOGY_TOPOLOGY_ADMIN_H_
#define SRC_MDS_TOPOLOGY_TOPOLOGY_ADMIN_H_

#include <vector>
#include <memory>
#include <functional>

#include "src/mds/topology/topology.h"
#include "proto/nameserver2.pb.h"


namespace curve {
namespace mds {
namespace topology {


struct CopysetIdInfo {
    PoolIdType logicalPoolId;
    CopySetIdType copySetId;
};

class TopologyAdmin {
 public:
    TopologyAdmin() {}
    virtual ~TopologyAdmin() {}
    virtual bool AllocateChunkRandomInSingleLogicalPool(
        ::curve::mds::FileType fileType,
        uint32_t chunkNumer,
        std::vector<CopysetIdInfo> *infos) = 0;
};


class TopologyAdminImpl : public TopologyAdmin {
 public:
    explicit TopologyAdminImpl(std::shared_ptr<Topology> topology)
    : topology_(topology) {
        std::srand(std::time(nullptr));
    }
    ~TopologyAdminImpl() {}


    /**
     * @brief 在单个逻辑池中随机分配若干个chunk
     *
     * @param fileType 文件类型
     * @param chunkNumber 分配chunk数
     * @param infos 分配到的copyset列表
     *
     * @retval true 分配成功
     * @retval false 分配失败
     */
    bool AllocateChunkRandomInSingleLogicalPool(
        curve::mds::FileType fileType,
        uint32_t chunkNumber,
        std::vector<CopysetIdInfo> *infos) override;

 private:
    std::shared_ptr<Topology> topology_;
};

/**
 * @brief chunk分配策略
 */
class AllocateChunkPolicy {
 public:
    AllocateChunkPolicy() {}
    /**
     * @brief  在单个逻辑池中随机分配若干个chunk
     *
     * @param copySetIds 指定逻辑池内的copysetId列表
     * @param logicalPoolId 逻辑池Id
     * @param chunkNumber 分配chunk数
     * @param infos 分配到的copyset列表
     *
     * @retval true 分配成功
     * @retval false 分配失败
     */
    static bool AllocateChunkRandomInSingleLogicalPool(
        std::vector<CopySetIdType> copySetIds,
        PoolIdType logicalPoolId,
        uint32_t chunkNumber,
        std::vector<CopysetIdInfo> *infos);
};



}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_ADMIN_H_

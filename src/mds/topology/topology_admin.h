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


    bool AllocateChunkRandomInSingleLogicalPool(
        curve::mds::FileType fileType,
        uint32_t chunkNumber,
        std::vector<CopysetIdInfo> *infos) override;


 private:
    using LogicalPoolFilter = std::function<bool (const LogicalPool &)>;
    void FilterLogicalPool(LogicalPoolFilter filter,
        std::vector<PoolIdType> *logicalPoolIdsOut);


 private:
    std::shared_ptr<Topology> topology_;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_TOPOLOGY_TOPOLOGY_ADMIN_H_

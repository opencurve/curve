/*
 * Project: curve
 * Created Date: Fri Oct 12 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_ADMIN_H_
#define CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_ADMIN_H_

#include <vector>
#include "proto/nameserver2.pb.h"

namespace curve {
namespace mds {
namespace topology {

#define PoolIdType  uint16_t
#define CopySetIdType uint32_t

struct CopysetIdInfo {
    PoolIdType logicalPoolId;
    CopySetIdType copySetId;
};

class TopologyAdmin {
public:
    TopologyAdmin() {}
    virtual ~TopologyAdmin() {}
    virtual bool AllocateChunkRandomInSingleLogicalPool (
        ::curve::mds::FileType fileType, 
        uint32_t chunkNumer,
        std::vector<CopysetIdInfo> *infos) = 0;
};

}  // namespace topology
}  // namespace mds
}  // namespace curve

#endif  // CURVE_SRC_MDS_TOPOLOGY_TOPOLOGY_ADMIN_H_
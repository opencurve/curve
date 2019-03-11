/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <vector>
#include "test/mds/schedule/common.h"
#include "proto/topology.pb.h"

namespace curve {
namespace mds {
namespace schedule {
::curve::mds::schedule::CopySetInfo GetCopySetInfoForTest() {
    PoolIdType pooId = 1;
    CopySetIdType copysetId = 1;
    CopySetKey copySetKey;
    copySetKey.first = pooId;
    copySetKey.second = copysetId;
    EpochType epoch = 1;
    ChunkServerIdType leader = 1;
    PeerInfo peer1(1, 1, 1, "192.168.10.1", 9000);
    PeerInfo peer2(2, 2, 2, "192.168.10.2", 9000);
    PeerInfo peer3(3, 3, 3, "192.168.10.3", 9000);
    std::vector<PeerInfo> peers({peer1, peer2, peer3});

    return ::curve::mds::schedule::CopySetInfo(
        copySetKey, epoch, leader, peers, ConfigChangeInfo{},
        CopysetStatistics{});
}

::curve::mds::topology::LogicalPool GetLogicalPoolForTest() {
    ::curve::mds::topology::LogicalPool::RedundanceAndPlaceMentPolicy policy;
    policy.pageFileRAP.replicaNum = 3;
    policy.pageFileRAP.zoneNum = 3;
    policy.pageFileRAP.copysetNum = 10;
    ::curve::mds::topology::LogicalPool::UserPolicy userPolicy;
    ::curve::mds::topology::LogicalPool logicalPool
        (1, "", 1, LogicalPoolType::PAGEFILE, policy, userPolicy, 1000, true);
    return logicalPool;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

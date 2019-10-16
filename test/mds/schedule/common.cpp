/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <sys/time.h>
#include <vector>
#include <set>
#include <string>
#include "test/mds/schedule/common.h"
#include "src/common/timeutility.h"
#include "proto/topology.pb.h"

using ::curve::mds::topology::ChunkServerStatus;

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

void GetCopySetInChunkServersForTest(
    std::map<ChunkServerIDType, std::vector<CopySetInfo>> *out) {
    PoolIdType poolId = 1;
    for (int i = 1; i <= 10; i++) {
        CopySetIdType copysetId = i;
        CopySetKey copySetKey;
        copySetKey.first = poolId;
        copySetKey.second = copysetId;
        EpochType epoch  = 1;
        ChunkServerIdType leader = i;
        PeerInfo peer1(i, 1, 1, "192.168.10.1", 9000 + i);
        PeerInfo peer2(i + 1, 2, 2, "192.168.10.2", 9000 + i + 1);
        PeerInfo peer3(i + 2, 3, 3, "192.168.10.3", 9000 + i + 2);
        std::vector<PeerInfo> peers({peer1, peer2, peer3});
        ::curve::mds::schedule::CopySetInfo copyset(copySetKey, epoch, leader,
            peers, ConfigChangeInfo{}, CopysetStatistics{});
        (*out)[i].emplace_back(copyset);
        (*out)[i + 1].emplace_back(copyset);
        (*out)[i + 2].emplace_back(copyset);
    }
}

::curve::mds::topology::CopySetInfo GetTopoCopySetInfoForTest() {
    ::curve::mds::topology::CopySetInfo topoCopy(1, 1);
    topoCopy.SetEpoch(1);
    topoCopy.SetLeader(1);
    topoCopy.SetCopySetMembers(std::set<ChunkServerIdType>{1, 2, 3});
    topoCopy.SetCandidate(4);
    return topoCopy;
}
std::vector<::curve::mds::topology::ChunkServer> GetTopoChunkServerForTest() {
    std::vector<::curve::mds::topology::ChunkServer> out;
    for (int i = 1; i <= 4; i++) {
        std::string ip = "192.168.10." + std::to_string(i);
        ::curve::mds::topology::ChunkServer chunkserver(
            i, "hello", "sata", i, ip,
            9000, "", ChunkServerStatus::READWRITE);
        chunkserver.SetChunkServerState(
            ::curve::mds::topology::ChunkServerState{});
        chunkserver.SetStartUpTime(
            ::curve::common::TimeUtility::GetTimeofDaySec());
        out.emplace_back(chunkserver);
    }
    return out;
}
std::vector<::curve::mds::topology::Server> GetServerForTest() {
    std::vector<::curve::mds::topology::Server> out;
    for (int i = 1; i <= 4; i++) {
        std::string ip = "192.168.10." + std::to_string(i);
        ::curve::mds::topology::Server server(
            i, "server", ip, 0, "", 0, i, 1, "");
        out.emplace_back(server);
    }
    return out;
}
::curve::mds::topology::LogicalPool GetPageFileLogicalPoolForTest() {
    ::curve::mds::topology::LogicalPool::RedundanceAndPlaceMentPolicy policy;
    policy.pageFileRAP.replicaNum = 3;
    policy.pageFileRAP.zoneNum = 3;
    policy.pageFileRAP.copysetNum = 10;
    ::curve::mds::topology::LogicalPool::UserPolicy userPolicy;
    ::curve::mds::topology::LogicalPool logicalPool
        (1, "", 1, LogicalPoolType::PAGEFILE, policy, userPolicy, 1000, true);
    return logicalPool;
}
::curve::mds::topology::LogicalPool GetAppendFileLogicalPoolForTest() {
    ::curve::mds::topology::LogicalPool::RedundanceAndPlaceMentPolicy policy;
    policy.appendFileRAP.replicaNum = 3;
    policy.appendFileRAP.zoneNum = 3;
    policy.appendFileRAP.copysetNum = 10;
    ::curve::mds::topology::LogicalPool::UserPolicy userPolicy;
    ::curve::mds::topology::LogicalPool logicalPool
        (1, "", 1, LogicalPoolType::APPENDFILE, policy, userPolicy, 1000, true);
    return logicalPool;
}
::curve::mds::topology::LogicalPool GetAppendECFileLogicalPoolForTest() {
    ::curve::mds::topology::LogicalPool::RedundanceAndPlaceMentPolicy policy;
    policy.appendECFileRAP.dSegmentNum = 3;
    policy.appendECFileRAP.zoneNum = 3;
    policy.appendECFileRAP.cSegmentNum = 10;
    ::curve::mds::topology::LogicalPool::UserPolicy userPolicy;
    ::curve::mds::topology::LogicalPool logicalPool(1, "", 1,
        LogicalPoolType::APPENDECFILE, policy, userPolicy, 1000, true);
    return logicalPool;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

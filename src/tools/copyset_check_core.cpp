/*
 * Project: curve
 * Created Date: 2019-10-30
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#include <math.h>
#include "src/tools/copyset_check_core.h"

DEFINE_uint64(margin, 1000, "The threshold of the gap between peers");
DEFINE_uint64(replicasNum, 3, "the number of replicas that required");
DEFINE_uint64(operatorMaxPeriod, 30, "max period of operator generating, "
                    "if no operators in a period, it considered to be healthy");
DEFINE_bool(checkOperator, false, "if true, the operator number of "
                                    "mds will be considered");

namespace curve {
namespace tool {

int CopysetCheckCore::Init(const std::string& mdsAddr) {
    return mdsClient_->Init(mdsAddr);
}

CopysetStatistics::CopysetStatistics(uint64_t total, uint64_t unhealthy)
            : totalNum(total), unhealthyNum(unhealthy) {
    if (total != 0) {
        unhealthyRatio =
            static_cast<double>(unhealthyNum) / totalNum;
    } else {
        unhealthyRatio = 0;
    }
}

int CopysetCheckCore::CheckOneCopyset(const PoolIdType& logicalPoolId,
                                  const CopySetIdType& copysetId) {
    Clear();
    std::vector<ChunkServerLocation> chunkserverLocation;
    int res = mdsClient_->GetChunkServerListInCopySet(logicalPoolId,
                                copysetId, &chunkserverLocation);
    if (res != 0) {
        std::cout << "GetChunkServerListInCopySet from mds fail!"
                  << std::endl;
        return -1;
    }
    bool isHealthy = true;
    for (const auto& csl : chunkserverLocation) {
        std::string hostIp = csl.hostip();
        uint64_t port = csl.port();
        std::string csAddr = hostIp + ":" + std::to_string(port);
        std::string groupId = ToGroupId(logicalPoolId, copysetId);
        butil::IOBuf iobuf;
        int res = QueryChunkServer(csAddr, &iobuf);
        if (res != 0) {
            // 如果查询chunkserver失败，认为不在线
            serviceExceptionChunkServers_.emplace(csAddr);
            isHealthy = false;
            continue;
        }
        std::vector<std::map<std::string, std::string>> copysetInfos;
        ParseResponseAttachment({groupId}, &iobuf, &copysetInfos);
        if (copysetInfos.empty()) {
            std::cout << "copyset not found on chunkserver " << csAddr
                      << ", may be transfered!" << std::endl;
            continue;
        }
        auto copysetInfo = copysetInfos[0];
        if (copysetInfo["state"] == "LEADER") {
            CheckResult res = CheckHealthOnLeader(&copysetInfo);
            if (res != CheckResult::kHealthy) {
                isHealthy = false;
            }
        } else {
            if (copysetInfo.count("leader") == 0 ||
                            copysetInfo["leader"] == kEmptyAddr) {
                isHealthy = false;
            }
        }
    }
    if (isHealthy) {
        return 0;
    }  else {
        return -1;
    }
}

int CopysetCheckCore::CheckCopysetsOnChunkServer(
                        const ChunkServerIdType& chunkserverId) {
    Clear();
    return CheckCopysetsOnChunkServer(chunkserverId, "");
}

int CopysetCheckCore::CheckCopysetsOnChunkServer(
                        const std::string& chunkserverAddr) {
    Clear();
    return CheckCopysetsOnChunkServer(0, chunkserverAddr);
}

int CopysetCheckCore::CheckCopysetsOnChunkServer(
                        const ChunkServerIdType& chunkserverId,
                        const std::string& chunkserverAddr) {
    curve::mds::topology::ChunkServerInfo csInfo;
    int res = 0;
    if (chunkserverId > 0) {
        res = mdsClient_->GetChunkServerInfo(chunkserverId, &csInfo);
    } else {
        res = mdsClient_->GetChunkServerInfo(chunkserverAddr, &csInfo);
    }
    if (res < 0) {
        std::cout << "GetChunkServerInfo from mds fail!" << std::endl;
        return -1;
    }
    // 如果chunkserver retired的话不发送请求
    if (csInfo.status() == ChunkServerStatus::RETIRED) {
        std::cout << "ChunkServer is retired!" << std::endl;
        return 0;
    }
    std::string hostIp = csInfo.hostip();
    uint64_t port = csInfo.port();
    std::string csAddr = hostIp + ":" + std::to_string(port);
    // 向chunkserver发送RPC请求获取raft state
    ChunkServerHealthStatus csStatus = CheckCopysetsOnChunkServer(csAddr, {});
    if (csStatus == ChunkServerHealthStatus::kHealthy) {
        return 0;
    } else {
        return -1;
    }
}

ChunkServerHealthStatus CopysetCheckCore::CheckCopysetsOnChunkServer(
                    const std::string& chunkserverAddr,
                    const std::set<std::string>& groupIds,
                    bool queryLeader) {
    bool isHealthy = true;
    butil::IOBuf iobuf;
    int res = QueryChunkServer(chunkserverAddr, &iobuf);
    if (res != 0) {
        // 如果查询chunkserver失败，认为不在线，把它上面所有的
        // copyset都添加到peerNotOnlineCopysets_里面
        UpdatePeerNotOnlineCopysets(chunkserverAddr);
        serviceExceptionChunkServers_.emplace(chunkserverAddr);
        return ChunkServerHealthStatus::kNotOnline;
    }
    // 存储每一个copyset的详细信息
    std::vector<std::map<std::string, std::string>> copysetInfos;
    ParseResponseAttachment(groupIds, &iobuf, &copysetInfos);
    // 对应的chunkserver上没有要找的leader的copyset，可能已经迁移出去了，
    // 但是follower这边还没更新，这种情况也认为chunkserver不健康
    if (copysetInfos.empty() ||
            (!groupIds.empty() && copysetInfos.size() != groupIds.size())) {
        std::cout << "Some copysets not found on chunkserver, may be tranfered"
                  << std::endl;
        return ChunkServerHealthStatus::kNotHealthy;
    }
    // 存储需要发送消息的chunkserver的地址和对应的groupId
    // key是chunkserver地址，value是groupId的列表
    std::map<std::string, std::set<std::string>> csAddrMap;
    for (auto& copysetInfo : copysetInfos) {
        std::string groupId = copysetInfo["groupId"];
        std::string state = copysetInfo["state"];
        if (state == "LEADER") {
            CheckResult res = CheckHealthOnLeader(&copysetInfo);
            switch (res) {
                case CheckResult::kPeersNoSufficient:
                    copysets_[kPeersNoSufficient].emplace(groupId);
                    isHealthy = false;
                    break;
                case CheckResult::kLogIndexGapTooBig:
                    copysets_[kLogIndexGapTooBig].emplace(groupId);
                    isHealthy = false;
                    break;
                case CheckResult::kInstallingSnapshot:
                    copysets_[kInstallingSnapshot].emplace(groupId);
                    isHealthy = false;
                    break;
                case CheckResult::kMinorityPeerNotOnline:
                    copysets_[kMinorityPeerNotOnline].emplace(groupId);
                    isHealthy = false;
                    break;
                case CheckResult::kMajorityPeerNotOnline:
                    copysets_[kMajorityPeerNotOnline].emplace(groupId);
                    isHealthy = false;
                    break;
                case CheckResult::kParseError:
                    std::cout << "Parse the result fail!" << std::endl;
                    isHealthy = false;
                    break;
                default:
                    break;
            }
        } else if (state == "FOLLOWER") {
            // 如果没有leader，返回不健康
            if (copysetInfo.count("leader") == 0 ||
                        copysetInfo["leader"] == kEmptyAddr) {
                copysets_[kNoLeader].emplace(groupId);
                isHealthy = false;
                continue;
            }
            if (queryLeader) {
                // 向leader发送rpc请求
                auto pos = copysetInfo["leader"].rfind(":");
                auto csAddr = copysetInfo["leader"].substr(0, pos);
                csAddrMap[csAddr].emplace(groupId);
            }
        } else if (state == "TRANSFERRING" || state == "CANDIDATE") {
            copysets_[kNoLeader].emplace(groupId);
            isHealthy = false;
        } else {
            // 其他情况有ERROR,UNINITIALIZED,SHUTTING和SHUTDOWN，这种都认为不健康，统计到
            // copyset里面
            std::string key = "state " + copysetInfo["state"];
            copysets_[key].emplace(groupId);
            isHealthy = false;
        }
    }
    // 遍历chunkserver发送请求
    for (const auto& item : csAddrMap) {
        ChunkServerHealthStatus res = CheckCopysetsOnChunkServer(item.first,
                                                           item.second);
        if (res != ChunkServerHealthStatus::kHealthy) {
            isHealthy = false;
        }
    }
    if (isHealthy) {
        return ChunkServerHealthStatus::kHealthy;
    } else {
        return ChunkServerHealthStatus::kNotHealthy;
    }
}

int CopysetCheckCore::CheckCopysetsOnServer(const ServerIdType& serverId,
                            std::vector<std::string>* unhealthyChunkServers) {
    Clear();
    return CheckCopysetsOnServer(serverId, "", true, unhealthyChunkServers);
}

int CopysetCheckCore::CheckCopysetsOnServer(const std::string& serverIp,
                            std::vector<std::string>* unhealthyChunkServers) {
    Clear();
    return CheckCopysetsOnServer(0, serverIp, true, unhealthyChunkServers);
}

int CopysetCheckCore::CheckCopysetsOnServer(const ServerIdType& serverId,
                                        const std::string& serverIp,
                                        bool queryLeader,
                            std::vector<std::string>* unhealthyChunkServers) {
    bool isHealthy = true;
    // 向mds发送RPC
    int res = 0;
    std::vector<ChunkServerInfo> chunkservers;
    if (serverId > 0) {
        res = mdsClient_->ListChunkServersOnServer(serverId, &chunkservers);
    } else {
        res = mdsClient_->ListChunkServersOnServer(serverIp, &chunkservers);
    }
    if (res < 0) {
        std::cout << "ListChunkServersOnServer fail!" << std::endl;
        return -1;
    }
    for (const auto& info : chunkservers) {
        std::string ip = info.hostip();
        uint64_t port = info.port();
        std::string csAddr = ip + ":" + std::to_string(port);
        ChunkServerHealthStatus res = CheckCopysetsOnChunkServer(csAddr,
                                                    {}, queryLeader);
        if (res != ChunkServerHealthStatus::kHealthy) {
            isHealthy = false;
            if (unhealthyChunkServers) {
                unhealthyChunkServers->emplace_back(csAddr);
            }
        }
    }
    if (isHealthy) {
        return 0;
    }  else {
        return -1;
    }
}

int CopysetCheckCore::CheckCopysetsInCluster() {
    Clear();
    bool isHealthy = true;
    std::vector<ServerInfo> servers;
    int res = mdsClient_->ListServersInCluster(&servers);
    if (res != 0) {
        std::cout << "ListServersInCluster fail!" << std::endl;
        return -1;
    }
    for (const auto& serverInfo : servers) {
        const auto& serverId = serverInfo.serverid();
        int res = CheckCopysetsOnServer(serverId, "", false);
        if (res != 0) {
            isHealthy = false;
        }
    }
    // 如果不健康，直接返回，如果健康，还需要对operator作出判断
    if (!isHealthy) {
        return -1;
    }
    // 默认不检查operator，在测试脚本之类的要求比较严格的地方才检查operator，不然
    // 每次执行命令等待30秒很不方便
    if (FLAGS_checkOperator) {
        int res = CheckOperator(kTotalOpName, FLAGS_operatorMaxPeriod);
        if (res != 0) {
            std::cout << "Exists operators on mds, scheduling!" << std::endl;
            return -1;
        }
    }
    return 0;
}

int CopysetCheckCore::CheckOperator(const std::string& opName,
                                    uint64_t checkTimeSec) {
    uint64_t startTime = curve::common::TimeUtility::GetTimeofDaySec();
    std::string metricName = GetOpNumMetricName(opName);
    do {
        uint64_t opNum = 0;
        int res = mdsClient_->GetMetric(metricName, &opNum);
        if (res != 0) {
            std::cout << "Get oparator num from mds fail!" << std::endl;
            return -1;
        }
        if (opNum != 0) {
            return opNum;
        }
        if (curve::common::TimeUtility::GetTimeofDaySec() -
                                        startTime >= checkTimeSec) {
            break;
        }
        sleep(1);
    } while (curve::common::TimeUtility::GetTimeofDaySec() -
                                        startTime < checkTimeSec);
    return 0;
}

std::string CopysetCheckCore::ToGroupId(const PoolIdType& logicPoolId,
                                           const CopySetIdType& copysetId) {
    uint64_t groupId = (static_cast<uint64_t>(logicPoolId) << 32) | copysetId;
    return std::to_string(groupId);
}


// 每个copyset的信息都会存储在一个map里面，map的key有
// groupId: 复制组的groupId
// peer_id: 10.182.26.45:8210:0格式的peer id
// state: 节点的状态，LEADER,FOLLOWER,CANDIDATE等等
// peers: 配置组里的成员，通过空格分隔
// last_log_id: 最后一个log entry的index
// leader: state为LEADER时才存在这个key，指向复制组leader
//
// replicator_1: 第一个follower的复制状态,value如下：
// next_index=6349842  flying_append_entries_size=0 idle hc=1234 ac=123 ic=0
//     next_index为下一个要发送给该follower的index
//     flying_append_entries_size是发出去还未确认的entry的数量
//     idle表明没有在安装快照，如果在安装快照的话是installing snapshot {12, 3},
//     1234和3分别是快照包含的最后一个log entry的index和term
//     hc,ac,ic分别是发向follower的heartbeat，append entry，
//     和install snapshot的rpc的数量
void CopysetCheckCore::ParseResponseAttachment(
                    const std::set<std::string>& gIds,
                    butil::IOBuf* iobuf,
                    std::vector<std::map<std::string, std::string>>* maps) {
    butil::IOBuf copyset;
    iobuf->append("\r\n");
    while (iobuf->cut_until(&copyset, "\r\n\r\n") == 0) {
        butil::IOBuf temp;
        std::string line;
        bool firstLine = true;
        int i = 0;
        copyset.append("\r\n");
        std::map<std::string, std::string> map;
        while (copyset.cut_until(&temp, "\r\n") == 0) {
            line = temp.to_string();
            if (firstLine) {
                firstLine = false;
                auto pos1 = line.find("[");
                auto pos2 = line.find("]");
                if (pos1 == line.npos || pos2 == line.npos) {
                    std::cout << "parse group id fail!" << std::endl;
                    break;
                }
                std::string gid = line.substr(pos1 + 1, pos2 - pos1 - 1);
                if (!gIds.empty() && gIds.count(gid) == 0) {
                    break;
                } else {
                    copysetsDetail_ += "\r\n";
                    copysetsDetail_ += ("[" + gid + "]\r\n");
                    copysetsDetail_ += copyset.to_string();
                    temp.clear();
                    map.emplace("groupId", gid);
                    if (gIds.empty()) {
                        // 查询chunkserver上所有copyset的时候保存，避免重复
                        copysets_[kTotal].emplace(gid);
                    }
                    continue;
                }
            }
            // 找到了copyset
            auto pos = line.npos;
            if (line.find("replicator") != line.npos) {
                pos = line.rfind(":");
            } else {
                pos = line.find(":");
            }
            if (pos == line.npos) {
                continue;
            }
            std::string key = line.substr(0, pos);
            // 如果是replicator，把key简化一下
            if (key.find("replicator") != key.npos) {
                key = "replicator" + std::to_string(i);
                ++i;
            }
            if (pos + 2 > (line.size() - 1)) {
                map.emplace(key, "");
            } else {
                map.emplace(key, line.substr(pos + 2));
            }
            temp.clear();
        }
        if (!map.empty()) {
            maps->push_back(map);
        }
        copyset.clear();
    }
}

int CopysetCheckCore::QueryChunkServer(const std::string& chunkserverAddr,
                                   butil::IOBuf* iobuf) {
    int res = csClient_->Init(chunkserverAddr);
    if (res != 0) {
        std::cout << "Init chunkserverClient fail!" << std::endl;
        chunkserverStatus_.emplace(chunkserverAddr, false);
        return -1;
    }
    res = csClient_->GetRaftStatus(iobuf);
    if (res == 0) {
        chunkserverStatus_.emplace(chunkserverAddr, true);
    } else {
        chunkserverStatus_.emplace(chunkserverAddr, false);
    }
    return res;
}

bool CopysetCheckCore::CheckChunkServerOnline(
                    const std::string& chunkserverAddr) {
    // 如果已经查询过，就直接返回结果，每次查询的总时间很短，
    // 假设这段时间内chunkserver不会恢复
    if (chunkserverStatus_.count(chunkserverAddr) != 0) {
        return chunkserverStatus_[chunkserverAddr];
    }
    int res = csClient_->Init(chunkserverAddr);
    if (res != 0) {
        std::cout << "Init chunkserverClient fail!" << std::endl;
        chunkserverStatus_.emplace(chunkserverAddr, false);
        return false;
    }
    bool online = csClient_->CheckChunkServerOnline();
    chunkserverStatus_.emplace(chunkserverAddr, online);
    return online;
}

CheckResult CopysetCheckCore::CheckHealthOnLeader(
                std::map<std::string, std::string>* map) {
    // 先判断peers是否小于3
    std::vector<std::string> peers;
    curve::common::SplitString((*map)["peers"], " ", &peers);
    if (peers.size() < FLAGS_replicasNum) {
        return CheckResult::kPeersNoSufficient;
    }
    // 检查不在线peer的数量
    uint32_t notOnlineNum = 0;
    for (const auto& peer : peers) {
        auto pos = peer.rfind(":");
        if (pos == peer.npos) {
            std::cout << "parse peer fail!" << std::endl;
            return CheckResult::kParseError;
        }
        std::string csAddr = peer.substr(0, pos);
        bool res = CheckChunkServerOnline(csAddr);
        if (!res) {
            serviceExceptionChunkServers_.emplace(csAddr);
            notOnlineNum++;
        }
    }
    if (notOnlineNum > 0) {
        uint32_t majority = peers.size() / 2 + 1;
        if (notOnlineNum < majority) {
            return CheckResult::kMinorityPeerNotOnline;
        } else {
            return CheckResult::kMajorityPeerNotOnline;
        }
    }
    // 根据replicator的情况判断log index之间的差距
    uint64_t lastLogId;
    std::string str = (*map)["storage"];
    auto pos1 = str.find("=");
    auto pos2 = str.find(",");
    if (pos1 == str.npos || pos2 == str.npos) {
        std::cout << "parse last log id fail!" << std::endl;
        return CheckResult::kParseError;
    }
    bool res = curve::common::StringToUll(str.substr(pos1 + 1, pos2 - pos1 - 1),
                                                        &lastLogId);
    if (!res) {
        std::cout << "parse last log id from string fail!" << std::endl;
        return CheckResult::kParseError;
    }
    uint64_t gap = 0;
    uint64_t nextIndex = 0;
    uint64_t flying = 0;
    for (uint32_t i = 0; i < peers.size() - 1; ++i) {
        std::string key = "replicator" + std::to_string(i);
        std::vector<std::string> repInfos;
        curve::common::SplitString((*map)[key], " ", &repInfos);
        for (auto info : repInfos) {
            auto pos = info.find("=");
            if (pos == info.npos) {
                if (info.find("snapshot") != info.npos) {
                    return CheckResult::kInstallingSnapshot;
                }
            }
            if (info.substr(0, pos) == "next_index") {
                res = curve::common::StringToUll(
                        info.substr(pos + 1), &nextIndex);
                if (!res) {
                    std::cout << "parse next index fail!" << std::endl;
                    return CheckResult::kParseError;
                }
            }
            if (info.substr(0, pos) == "flying_append_entries_size") {
                res = curve::common::StringToUll(info.substr(pos + 1),
                                                            &flying);
                if (!res) {
                    std::cout << "parse flying_size fail!" << std::endl;
                    return CheckResult::kParseError;
                }
            }
            gap = std::max(gap, lastLogId - (nextIndex - 1 - flying));
        }
    }
    if (gap > FLAGS_margin) {
        return CheckResult::kLogIndexGapTooBig;
    }
    return CheckResult::kHealthy;
}

void CopysetCheckCore::UpdatePeerNotOnlineCopysets(const std::string& csAddr) {
    std::vector<CopysetInfo> copysets;
    int res = mdsClient_->GetCopySetsInChunkServer(csAddr, &copysets);
    if (res != 0) {
        std::cout << "GetCopySetsInChunkServer " << csAddr
                  << " fail!" << std::endl;
        return;
    }

    std::vector<CopySetIdType> copysetIds;
    PoolIdType logicalPoolId = copysets[0].logicalpoolid();
    for (const auto& csInfo : copysets) {
        copysetIds.emplace_back(csInfo.copysetid());
    }

    // 获取每个copyset的成员
    std::vector<CopySetServerInfo> csServerInfos;
    res = mdsClient_->GetChunkServerListInCopySets(logicalPoolId,
                                                   copysetIds,
                                                   &csServerInfos);
    if (res != 0) {
        std::cout << "GetChunkServerListInCopySets fail" << std::endl;
        return;
    }
    // 遍历每个copyset
    for (const auto& info : csServerInfos) {
        uint32_t offlineNum = 0;
        for (const auto& csLoc : info.cslocs()) {
            std::string addr = csLoc.hostip() + ":"
                               + std::to_string(csLoc.port());
            if (!CheckChunkServerOnline(addr)) {
                offlineNum++;
            }
        }
        CopySetIdType copysetId = info.copysetid();
        std::string groupId = ToGroupId(logicalPoolId,
                                        copysetId);
        uint32_t majority = info.cslocs().size() / 2 + 1;
        if (offlineNum < majority) {
            copysets_[kMinorityPeerNotOnline].emplace(groupId);
        } else {
            copysets_[kMajorityPeerNotOnline].emplace(groupId);
        }
        copysets_[kTotal].emplace(groupId);
    }
}

CopysetStatistics CopysetCheckCore::GetCopysetStatistics() {
    uint64_t total = 0;
    std::set<std::string> unhealthyCopysets;
    for (const auto& item : copysets_) {
        if (item.first == kTotal) {
            total = item.second.size();
        } else {
            // 求并集
            unhealthyCopysets.insert(item.second.begin(),
                                     item.second.end());
        }
    }
    uint64_t unhealthyNum = unhealthyCopysets.size();
    CopysetStatistics statistics(total, unhealthyNum);
    return statistics;
}

void CopysetCheckCore::Clear() {
    copysets_.clear();
    serviceExceptionChunkServers_.clear();
    chunkserverStatus_.clear();
    copysetsDetail_.clear();
}
}  // namespace tool
}  // namespace curve

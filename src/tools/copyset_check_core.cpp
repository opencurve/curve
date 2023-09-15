/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 2019-10-30
 * Author: charisu
 */
#include "src/tools/copyset_check_core.h"
#include <math.h>
#include <cstdint>

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

CheckResult CopysetCheckCore::CheckOneCopyset(const PoolIdType& logicalPoolId,
                                  const CopySetIdType& copysetId) {
    Clear();
    std::vector<ChunkServerLocation> chunkserverLocation;
    int res = mdsClient_->GetChunkServerListInCopySet(logicalPoolId,
                                copysetId, &chunkserverLocation);
    if (res != 0) {
        std::cout << "GetChunkServerListInCopySet from mds fail!"
                  << std::endl;
        return CheckResult::kOtherErr;
    }
    int majority = chunkserverLocation.size() / 2 + 1;
    int offlinePeers = 0;
    CheckResult checkRes = CheckResult::kHealthy;
    for (const auto& csl : chunkserverLocation) {
        std::string hostIp = csl.hostip();
        uint64_t port = csl.port();
        std::string csAddr = hostIp + ":" + std::to_string(port);
        std::string groupId = ToGroupId(logicalPoolId, copysetId);
        butil::IOBuf iobuf;
        int res = QueryChunkServer(csAddr, &iobuf);
        if (res != 0) {
            // If the query for chunkserver fails, it is considered offline
            serviceExceptionChunkServers_.emplace(csAddr);
            chunkserverCopysets_[csAddr] = {};
            ++offlinePeers;
            continue;
        }
        std::vector<std::map<std::string, std::string>> copysetInfos;
        ParseResponseAttachment({groupId}, &iobuf, &copysetInfos, true);
        if (copysetInfos.empty()) {
            std::cout << "copyset not found on chunkserver " << csAddr
                      << std::endl;
            copysetLoacExceptionChunkServers_.emplace(csAddr);
            ++offlinePeers;
            continue;
        }
        auto copysetInfo = copysetInfos[0];
        if (copysetInfo[kState] == kStateLeader) {
            CheckResult res = CheckHealthOnLeader(&copysetInfo);
            if (res != CheckResult::kHealthy) {
                return res;
            }
        } else {
            if (copysetInfo.count(kLeader) == 0 ||
                            copysetInfo[kLeader] == kEmptyAddr) {
                checkRes = CheckResult::kOtherErr;
            }
        }
    }
    if (offlinePeers >= majority) {
        checkRes = CheckResult::kMajorityPeerNotOnline;
    } else if (offlinePeers > 0) {
        checkRes = CheckResult::kMinorityPeerNotOnline;
    }
    return checkRes;
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
    // If chunkserver is redirected, do not send the request
    if (csInfo.status() == ChunkServerStatus::RETIRED) {
        std::cout << "ChunkServer is retired!" << std::endl;
        return 0;
    }
    std::string hostIp = csInfo.hostip();
    uint64_t port = csInfo.port();
    std::string csAddr = hostIp + ":" + std::to_string(port);
    // Send RPC request to chunkserver to obtain raft state
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
                                bool queryLeader,
                                std::pair<int, butil::IOBuf> *record,
                                bool queryCs) {
    bool isHealthy = true;
    int res = 0;
    butil::IOBuf iobuf;
    if (queryCs) {
        res = QueryChunkServer(chunkserverAddr, &iobuf);
    } else {
        res = record->first;
        iobuf = record->second;
    }

    if (res != 0) {
        // If querying the chunkserver fails, consider it offline and add all its 
        // copysets to the peerNotOnlineCopysets_.
        UpdatePeerNotOnlineCopysets(chunkserverAddr);
        serviceExceptionChunkServers_.emplace(chunkserverAddr);
        chunkserverCopysets_[chunkserverAddr] = {};
        return ChunkServerHealthStatus::kNotOnline;
    }
    // Store detailed information for each copyset
    CopySetInfosType copysetInfos;
    ParseResponseAttachment(groupIds, &iobuf, &copysetInfos);
    // Only update the copyset list on chunkServer when querying all chunkservers
    if (groupIds.empty()) {
        UpdateChunkServerCopysets(chunkserverAddr, copysetInfos);
    }

    // There is no copyset for the leader you are looking for on the corresponding chunkserver, it may have already been migrated,
    // But the follower has not been updated yet, and this situation also suggests that chunkserver is unhealthy
    if (copysetInfos.empty() ||
            (!groupIds.empty() && copysetInfos.size() != groupIds.size())) {
        std::cout << "Some copysets not found on chunkserver, may be tranfered"
                  << std::endl;
        return ChunkServerHealthStatus::kNotHealthy;
    }
    // Store the address and corresponding groupId of the chunkserver that needs to send messages
    // Key is the chunkserver address, and value is a list of groupIds
    std::map<std::string, std::set<std::string>> csAddrMap;
    // Store the peers corresponding to the copyset without a leader, with key as groupId and value as configuration
    std::map<std::string, std::vector<std::string>> noLeaderCopysetsPeers;
    for (auto& copysetInfo : copysetInfos) {
        std::string groupId = copysetInfo[kGroupId];
        std::string state = copysetInfo[kState];
        copysets_[kTotal].emplace(groupId);
        if (state == kStateLeader) {
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
        } else if (state == kStateFollower) {
            // If there is no leader, check if most are offline
            // If yes, mark it as mostly offline, otherwise mark it as No leader
            if (copysetInfo.count(kLeader) == 0 ||
                        copysetInfo[kLeader] == kEmptyAddr) {
                std::vector<std::string> peers;
                curve::common::SplitString(copysetInfo[kPeers], " ", &peers);
                noLeaderCopysetsPeers[groupId] = peers;
                continue;
            }
            if (queryLeader) {
                // Send an rpc request to the leader
                auto pos = copysetInfo[kLeader].rfind(":");
                auto csAddr = copysetInfo[kLeader].substr(0, pos);
                csAddrMap[csAddr].emplace(groupId);
            }
        } else if (state == kStateTransferring || state == kStateCandidate) {
            copysets_[kNoLeader].emplace(groupId);
            isHealthy = false;
        } else {
            // In other cases such as ERROR, UNINITIALIZED, SHUTTING, and SHUTDOWN, 
            // they are considered unhealthy and are counted within the copyset.
            std::string key = "state " + copysetInfo[kState];
            copysets_[key].emplace(groupId);
            isHealthy = false;
        }
    }

    // Traverse copysets without leaders
    bool health = CheckCopysetsNoLeader(chunkserverAddr,
                                        noLeaderCopysetsPeers);
    if (!health) {
        isHealthy = false;
    }

    // Traverse chunkserver to send requests
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

bool CopysetCheckCore::CheckCopysetsNoLeader(const std::string& csAddr,
                                             const std::map<std::string,
                                             std::vector<std::string>>&
                                             copysetsPeers) {
    if (copysetsPeers.empty()) {
        return true;
    }
    std::set<std::string> groupIds;
    for (const auto& item : copysetsPeers) {
        groupIds.emplace(item.first);
    }
    bool isHealthy = true;
    std::map<std::string, bool> result;
    int res = CheckIfChunkServerInCopysets(csAddr, groupIds, &result);
    if (res != 0) {
        std::cout << "CheckIfChunkServerInCopysets fail!" << std::endl;
        return false;
    }
    for (const auto& item : result) {
        // If in the configuration group, check if it is a majority offline
        if (item.second) {
            isHealthy = false;
            std::string groupId = item.first;
            CheckResult checkRes = CheckPeerOnlineStatus(
                                        groupId,
                                        copysetsPeers.at(item.first));
            if (checkRes == CheckResult::kMajorityPeerNotOnline) {
                copysets_[kMajorityPeerNotOnline].emplace(groupId);
                continue;
            }
            copysets_[kNoLeader].emplace(groupId);
        }
    }
    return isHealthy;
}

int CopysetCheckCore::CheckIfChunkServerInCopysets(const std::string& csAddr,
                                    const std::set<std::string> copysets,
                                    std::map<std::string, bool>* result) {
    PoolIdType logicPoolId;
    std::vector<CopySetIdType> copysetIds;
    for (const auto& gId : copysets) {
        uint64_t groupId;
        if (!curve::common::StringToUll(gId, &groupId)) {
            std::cout << "parse group id fail: " << groupId << std::endl;
            continue;
        }
        logicPoolId = GetPoolID(groupId);
        CopySetIdType copysetId = GetCopysetID(groupId);
        copysetIds.push_back(copysetId);
    }

    std::vector<CopySetServerInfo> csServerInfos;
    int res = mdsClient_->GetChunkServerListInCopySets(logicPoolId,
                                                copysetIds, &csServerInfos);
    if (res != 0) {
        std::cout << "GetChunkServerListInCopySets fail!" << std::endl;
        return res;
    }
    for (const auto& info : csServerInfos) {
        CopySetIdType copysetId = info.copysetid();
        std::string groupId = ToGroupId(logicPoolId, copysetId);
        for (const auto& csLoc : info.cslocs()) {
            std::string addr = csLoc.hostip() + ":"
                               + std::to_string(csLoc.port());
            if (addr == csAddr) {
                (*result)[groupId] = true;
                break;
            }
        }
    }
    return 0;
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

void CopysetCheckCore::ConcurrentCheckCopysetsOnServer(
                const std::vector<ChunkServerInfo> &chunkservers,
                uint32_t *index, std::map<std::string,
                std::pair<int, butil::IOBuf>> *result) {
    while (1) {
        indexMutex.lock();
        if (*index + 1 > chunkservers.size()) {
            indexMutex.unlock();
            break;
        }
        auto info = chunkservers[*index];
        (*index)++;
        indexMutex.unlock();
        std::string csAddr = info.hostip() + ":" + std::to_string(info.port());
        butil::IOBuf iobuf;
        int res = QueryChunkServer(csAddr, &iobuf);

        mapMutex.lock();
        result->emplace(csAddr, std::make_pair(res, iobuf));
        mapMutex.unlock();
    }
}

int CopysetCheckCore::CheckCopysetsOnServer(const ServerIdType& serverId,
                            const std::string& serverIp, bool queryLeader,
                            std::vector<std::string>* unhealthyChunkServers) {
    bool isHealthy = true;
    // Send RPC to mds
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
    std::vector<Thread> threadpool;
    std::map<std::string, std::pair<int, butil::IOBuf>> queryCsResult;
    uint32_t index = 0;
    for (uint64_t i = 0; i < FLAGS_rpcConcurrentNum; i++) {
        threadpool.emplace_back(Thread(
                        &CopysetCheckCore::ConcurrentCheckCopysetsOnServer,
                        this, std::ref(chunkservers), &index,
                        &queryCsResult));
    }
    for (auto &thread : threadpool) {
        thread.join();
    }

    for (auto &record : queryCsResult) {
        std::string chunkserverAddr = record.first;
        auto res = CheckCopysetsOnChunkServer(chunkserverAddr, {}, queryLeader,
                                              &record.second, false);
        if (res != ChunkServerHealthStatus::kHealthy) {
            isHealthy = false;
            if (unhealthyChunkServers) {
                unhealthyChunkServers->emplace_back(chunkserverAddr);
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
    // Check if the number of copysets obtained from chunkserver matches the number of mds records
    res = CheckCopysetsWithMds();
    if (res != 0) {
        std::cout << "CheckCopysetNumWithMds fail!" << std::endl;
        return -1;
    }
    // If not healthy, return directly. If healthy, make a judgment on the operator
    if (!isHealthy) {
        return -1;
    }
    // By default, operators are not checked, and only checked in areas with strict requirements such as test scripts, otherwise
    // waiting for 30 seconds each time executing a command is inconvenient
    if (FLAGS_checkOperator) {
        int res = CheckOperator(kTotalOpName, FLAGS_operatorMaxPeriod);
        if (res != 0) {
            std::cout << "Exists operators on mds, scheduling!" << std::endl;
            return -1;
        }
    }
    return 0;
}

int CopysetCheckCore::CheckCopysetsWithMds() {
    std::vector<CopysetInfo> copysetsInMds;
    int res = mdsClient_->GetCopySetsInCluster(&copysetsInMds);
    if (res != 0) {
        std::cout << "GetCopySetsInCluster fail!" << std::endl;
        return -1;
    }
    if (copysetsInMds.size() != copysets_[kTotal].size()) {
        std::cout << "Copyset numbers in chunkservers not consistent"
                     " with mds, please check! copysets on chunkserver: "
                     << copysets_[kTotal].size() << ", copysets in mds: "
                     << copysetsInMds.size() << std::endl;
        return -1;
    }
    std::set<std::string> copysetsInMdsGid;
    for (const auto& copyset : copysetsInMds) {
        std::string gId = ToGroupId(copyset.logicalpoolid(),
                                    copyset.copysetid());
        copysetsInMdsGid.insert(gId);
    }
    int ret = 0;
    std::vector<std::string> copysetsInMdsNotInCs(10);
    auto iter = std::set_difference(copysetsInMdsGid.begin(),
                    copysetsInMdsGid.end(), copysets_[kTotal].begin(),
                    copysets_[kTotal].end(), copysetsInMdsNotInCs.begin());
    copysetsInMdsNotInCs.resize(iter - copysetsInMdsNotInCs.begin());
    if (!copysetsInMdsNotInCs.empty()) {
        std::cout << "There are " << copysetsInMdsNotInCs.size()
                  << " copysets on mds not found on chunkserver, defail:";
        for (const auto& copyset : copysetsInMdsNotInCs) {
            std::cout << " " << copyset;
        }
        std::cout << std::endl;
        ret = -1;
    }
    std::vector<std::string> copysetsInCsNotInMds(10);
    iter = std::set_difference(copysets_[kTotal].begin(),
                copysets_[kTotal].end(), copysetsInMdsGid.begin(),
                    copysetsInMdsGid.end(), copysetsInCsNotInMds.begin());
    copysetsInCsNotInMds.resize(iter - copysetsInCsNotInMds.begin());
    if (!copysetsInCsNotInMds.empty()) {
        std::cout << "There are " << copysetsInCsNotInMds.size()
                  << " copysets on chunkserver not found on Mds, defail:";
        for (const auto& copyset : copysetsInCsNotInMds) {
            std::cout << " " << copyset;
        }
        std::cout << std::endl;
        ret = -1;
    }

    // Check scan status for inconsistent copyset
    auto nInconsistent = CheckScanStatus(copysetsInMds);
    if (nInconsistent > 0) {
        std::cout << "There are " << nInconsistent << " inconsistent copyset"
                  << std::endl;
        ret = -1;
    }

    return ret;
}

int CopysetCheckCore::CheckScanStatus(
    const std::vector<CopysetInfo>& copysetInfos) {
    int count = 0;
    for (auto& copysetInfo : copysetInfos) {
        if (!copysetInfo.has_lastscanconsistent() ||
            copysetInfo.lastscanconsistent()) {
            continue;
        }

        auto groupId = ToGroupId(copysetInfo.logicalpoolid(),
                                 copysetInfo.copysetid());
        copysets_[kThreeCopiesInconsistent].emplace(groupId);
        count++;
    }

    return count;
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

// Information for each copyset is stored in a map. The map's keys include:
// - groupId: The groupId of the replication group.
// - peer_id: The peer id in the format 10.182.26.45:8210:0.
// - state: The node's state, which can be LEADER, FOLLOWER, CANDIDATE, etc.
// - peers: Members in the configuration group, separated by spaces.
// - last_log_id: The index of the last log entry.
// - leader: This key exists only when the state is LEADER and points to the leader of the replication group.
//
// replicator_1: The replication status of the first follower, with values as follows:
// next_index=6349842 flying_append_entries_size=0 idle hc=1234 ac=123 ic=0
//     - next_index: The next index to be sent to this follower.
//     - flying_append_entries_size: The number of unconfirmed entries that have been sent.
//     - idle: Indicates whether there is no snapshot installation. If a snapshot is being installed, it will show as "installing snapshot {12, 3}",
//       where 1234 and 3 are the last log entry's index and term included in the snapshot.
//     - hc, ac, ic: The counts of RPCs sent to the follower for heartbeat, append entry, and install snapshot, respectively.
void CopysetCheckCore::ParseResponseAttachment(
                    const std::set<std::string>& gIds,
                    butil::IOBuf* iobuf,
                    CopySetInfosType* copysetInfos,
                    bool saveIobufStr) {
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
                    if (saveIobufStr) {
                        copysetsDetail_ += "\r\n";
                        copysetsDetail_ += ("[" + gid + "]\r\n");
                        copysetsDetail_ += copyset.to_string();
                    }
                    temp.clear();
                    map.emplace(kGroupId, gid);
                    continue;
                }
            }
            // Found copyset
            auto pos = line.npos;
            if (line.find(kReplicator) != line.npos) {
                pos = line.rfind(":");
            } else {
                pos = line.find(":");
            }
            if (pos == line.npos) {
                continue;
            }
            std::string key = line.substr(0, pos);
            // If it's a replicator, simplify the key
            if (key.find(kReplicator) != key.npos) {
                key = kReplicator + std::to_string(i);
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
            copysetInfos->push_back(map);
        }
        copyset.clear();
    }
}

int CopysetCheckCore::QueryChunkServer(const std::string& chunkserverAddr,
                                   butil::IOBuf* iobuf) {
    // unit test will set csClient_ to mock
    auto csClient = (csClient_ == nullptr) ?
                     std::make_shared<ChunkServerClient>() : csClient_;
    int res = csClient->Init(chunkserverAddr);
    if (res != 0) {
        std::cout << "Init chunkserverClient fail!" << std::endl;
        return -1;
    }
    return csClient->GetRaftStatus(iobuf);
}

void CopysetCheckCore::UpdateChunkServerCopysets(
                        const std::string& csAddr,
                        const CopySetInfosType& copysetInfos) {
    std::set<std::string> copysetIds;
    for (const auto& copyset : copysetInfos) {
        copysetIds.emplace(copyset.at(kGroupId));
    }
    chunkserverCopysets_[csAddr] = copysetIds;
}

// Check if chunkserver is online by sending RPC
bool CopysetCheckCore::CheckChunkServerOnline(
                    const std::string& chunkserverAddr) {
    auto csClient = (csClient_ == nullptr) ?
                     std::make_shared<ChunkServerClient>() : csClient_;
    int res = csClient->Init(chunkserverAddr);
    if (res != 0) {
        std::cout << "Init chunkserverClient fail!" << std::endl;
        chunkserverCopysets_[chunkserverAddr] = {};
        return false;
    }
    bool online = csClient->CheckChunkServerOnline();
    if (!online) {
        chunkserverCopysets_[chunkserverAddr] = {};
    }
    return online;
}

bool CopysetCheckCore::CheckCopySetOnline(const std::string& csAddr,
                                          const std::string& groupId) {
    if (chunkserverCopysets_.count(csAddr) != 0) {
        const auto& copysets = chunkserverCopysets_[csAddr];
        if (copysets.empty()) {
            return false;
        }
        bool online = (copysets.find(groupId) != copysets.end());
        if (online) {
            return true;
        } else {
            copysetLoacExceptionChunkServers_.emplace(csAddr);
            return false;
        }
    }
    butil::IOBuf iobuf;
    int res = QueryChunkServer(csAddr, &iobuf);
    if (res != 0) {
        // If the query for chunkserver fails, it is considered offline
        serviceExceptionChunkServers_.emplace(csAddr);
        chunkserverCopysets_[csAddr] = {};
        return false;
    }
    CopySetInfosType copysetInfos;
    ParseResponseAttachment({}, &iobuf, &copysetInfos);
    UpdateChunkServerCopysets(csAddr, copysetInfos);
    bool online = (chunkserverCopysets_[csAddr].find(groupId) !=
                                    chunkserverCopysets_[csAddr].end());
    if (!online) {
        copysetLoacExceptionChunkServers_.emplace(csAddr);
    }
    return online;
}

CheckResult CopysetCheckCore::CheckPeerOnlineStatus(
                            const std::string& groupId,
                            const std::vector<std::string>& peers) {
    int notOnlineNum = 0;
    for (const auto& peer : peers) {
        auto pos = peer.rfind(":");
        if (pos == peer.npos) {
            std::cout << "parse peer fail!" << std::endl;
            return CheckResult::kParseError;
        }
        std::string csAddr = peer.substr(0, pos);
        bool online = CheckCopySetOnline(csAddr, groupId);
        if (!online) {
            notOnlineNum++;
        }
    }
    if (notOnlineNum > 0) {
        uint32_t majority = peers.size() / 2 + 1;
        if (notOnlineNum < static_cast<int>(majority)) {
            return CheckResult::kMinorityPeerNotOnline;
        } else {
            return CheckResult::kMajorityPeerNotOnline;
        }
    }
    return CheckResult::kHealthy;
}

CheckResult CopysetCheckCore::CheckHealthOnLeader(
                std::map<std::string, std::string>* map) {
    // First, determine if the peers are less than 3
    std::vector<std::string> peers;
    curve::common::SplitString((*map)[kPeers], " ", &peers);
    if (peers.size() < FLAGS_replicasNum) {
        return CheckResult::kPeersNoSufficient;
    }
    std::string groupId = (*map)[kGroupId];
    // Check the number of offline peers
    CheckResult checkRes = CheckPeerOnlineStatus(groupId, peers);
    if (checkRes != CheckResult::kHealthy) {
        return checkRes;
    }
    // Judging the gap between log indices based on the replicator's situation
    uint64_t lastLogId;
    std::string str = (*map)[kStorage];
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
        std::string key = kReplicator + std::to_string(i);
        std::vector<std::string> repInfos;
        curve::common::SplitString((*map)[key], " ", &repInfos);
        for (auto info : repInfos) {
            auto pos = info.find("=");
            if (pos == info.npos) {
                if (info.find(kSnapshot) != info.npos) {
                    return CheckResult::kInstallingSnapshot;
                }
            }
            if (info.substr(0, pos) == kNextIndex) {
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
            if (lastLogId > (nextIndex - 1 - flying)) {
                gap = std::max(gap, lastLogId - (nextIndex - 1 - flying));
            }
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
    } else if (copysets.empty()) {
        std::cout << "No copysets on chunkserver " << csAddr << std::endl;
        return;
    }

    std::vector<CopySetIdType> copysetIds;
    PoolIdType logicalPoolId = copysets[0].logicalpoolid();
    for (const auto& csInfo : copysets) {
        copysetIds.emplace_back(csInfo.copysetid());
    }

    // Get the members of each copyset
    std::vector<CopySetServerInfo> csServerInfos;
    res = mdsClient_->GetChunkServerListInCopySets(logicalPoolId,
                                                   copysetIds,
                                                   &csServerInfos);
    if (res != 0) {
        std::cout << "GetChunkServerListInCopySets fail" << std::endl;
        return;
    }
    // Traverse each copyset
    for (const auto& info : csServerInfos) {
        std::vector<std::string> peers;
        for (const auto& csLoc : info.cslocs()) {
            std::string peer = csLoc.hostip() + ":"
                               + std::to_string(csLoc.port()) + ":0";
            peers.emplace_back(peer);
        }
        CopySetIdType copysetId = info.copysetid();
        std::string groupId = ToGroupId(logicalPoolId,
                                        copysetId);
        CheckResult checkRes = CheckPeerOnlineStatus(groupId, peers);
        if (checkRes == CheckResult::kMinorityPeerNotOnline) {
            copysets_[kMinorityPeerNotOnline].emplace(groupId);
        } else if (checkRes == CheckResult::kMajorityPeerNotOnline) {
            copysets_[kMajorityPeerNotOnline].emplace(groupId);
        } else {
            std::cout << "CheckPeerOnlineStatus met error!" << std::endl;
            continue;
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
            // Union
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
    chunkserverCopysets_.clear();
    copysetsDetail_.clear();
}

int CopysetCheckCore::ListMayBrokenVolumes(
                    std::vector<std::string>* fileNames) {
    int res = CheckCopysetsOnOfflineChunkServer();
    if (res != 0) {
        std::cout << "CheckCopysetsOnOfflineChunkServer fail" << std::endl;
        return -1;
    }
    std::vector<CopysetInfo> copysets;
    GetCopysetInfos(kMajorityPeerNotOnline, &copysets);
    if (copysets.empty()) {
        std::cout << "No majority-peers-offline copysets" << std::endl;
        return 0;
    }
    res = mdsClient_->ListVolumesOnCopyset(copysets, fileNames);
    if (res != 0) {
        std::cout << "ListVolumesOnCopyset fail" << std::endl;
        return -1;
    }
    return 0;
}

void CopysetCheckCore::GetCopysetInfos(const char* key,
                                std::vector<CopysetInfo>* copysets) {
    (void)key;
    for (auto iter = copysets_[kMajorityPeerNotOnline].begin();
                    iter != copysets_[kMajorityPeerNotOnline].end(); ++iter) {
        std::string gid = *iter;
        uint64_t groupId;
        if (!curve::common::StringToUll(gid, &groupId)) {
            std::cout << "parse group id fail: " << groupId << std::endl;
            continue;
        }
        PoolIdType lgId = GetPoolID(groupId);
        CopySetIdType csId = GetCopysetID(groupId);
        common::CopysetInfo copyset;
        copyset.set_logicalpoolid(lgId);
        copyset.set_copysetid(csId);
        copysets->emplace_back(copyset);
    }
}

int CopysetCheckCore::CheckCopysetsOnOfflineChunkServer() {
    std::vector<ChunkServerInfo> chunkservers;
    int res = mdsClient_->ListChunkServersInCluster(&chunkservers);
    if (res != 0) {
        std::cout << "ListChunkServersInCluster fail" << std::endl;
        return -1;
    }
    for (const auto& cs : chunkservers) {
        std::string csAddr = cs.hostip() + ":" + std::to_string(cs.port());
        if (!CheckChunkServerOnline(csAddr)) {
            UpdatePeerNotOnlineCopysets(csAddr);
        }
    }
    return 0;
}

}  // namespace tool
}  // namespace curve

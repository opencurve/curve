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
 * Created Date: 2019-12-03
 * Author: charisu
 */
#include "src/tools/status_tool.h"
#include <utility>

DEFINE_bool(offline, false, "if true, only list offline chunskervers");
DEFINE_bool(unhealthy, false, "if true, only list chunkserver that unhealthy "
                              "ratio greater than 0");
DEFINE_bool(checkHealth, true, "if true, it will check the health "
                                "state of chunkserver in chunkserver-list");
DEFINE_bool(checkCSAlive, false, "if true, it will check the online state of "
                                "chunkservers with rpc in chunkserver-list");
DEFINE_bool(listClientInRepo, true, "if true, list-client will list all clients"
                                    " include that in repo");
DEFINE_uint64(walSegmentSize, 8388608, "wal segment size");
DECLARE_string(mdsAddr);
DECLARE_string(etcdAddr);
DECLARE_string(mdsDummyPort);
DECLARE_bool(detail);

namespace curve {
namespace tool {

std::ostream& operator<<(std::ostream& os,
                    std::vector<std::string> strs) {
    for (uint32_t i = 0; i < strs.size(); ++i) {
        if (i != 0) {
            os << ", ";
        }
        os << strs[i];
    }
    return os;
}

std::string ToString(ServiceName name) {
    static std::map<ServiceName, std::string> serviceNameMap =
                           {{ServiceName::kMds, "mds"},
                            {ServiceName::kEtcd, "etcd"},
                            {ServiceName::kSnapshotCloneServer,
                                       "snapshot-clone-server"}};
    return serviceNameMap[name];
}

int StatusTool::Init(const std::string& command) {
    if (CommandNeedMds(command) && !mdsInited_) {
        if (mdsClient_->Init(FLAGS_mdsAddr, FLAGS_mdsDummyPort) != 0) {
            std::cout << "Init mdsClient failed!" << std::endl;
            return -1;
        }
        if (copysetCheckCore_->Init(FLAGS_mdsAddr) != 0) {
            std::cout << "Init copysetCheckCore failed!" << std::endl;
            return -1;
        }
        mdsInited_ = true;
    }
    if (CommandNeedEtcd(command) && !etcdInited_) {
        if (etcdClient_->Init(FLAGS_etcdAddr) != 0) {
            std::cout << "Init etcdClient failed!" << std::endl;
            return -1;
        }
        etcdInited_ = true;
    }
    if (CommandNeedSnapshotClone(command)) {
        if (snapshotClient_->Init(FLAGS_snapshotCloneAddr,
                                  FLAGS_snapshotCloneDummyPort) != 0) {
            std::cout << "Init snapshotClient failed!" << std::endl;
            return -1;
        }
    }
    return 0;
}

bool StatusTool::CommandNeedEtcd(const std::string& command) {
    return (command == kEtcdStatusCmd || command == kStatusCmd);
}

bool StatusTool::CommandNeedMds(const std::string& command) {
    return (command != kEtcdStatusCmd && command != kSnapshotCloneStatusCmd);
}

bool StatusTool::CommandNeedSnapshotClone(const std::string& command) {
    return (command == kSnapshotCloneStatusCmd || command == kStatusCmd);
}

bool StatusTool::SupportCommand(const std::string& command) {
    return (command == kSpaceCmd || command == kStatusCmd
                                 || command == kChunkserverListCmd
                                 || command == kChunkserverStatusCmd
                                 || command == kMdsStatusCmd
                                 || command == kEtcdStatusCmd
                                 || command == kClientStatusCmd
                                 || command == kClientListCmd
                                 || command == kSnapshotCloneStatusCmd
                                 || command == kClusterStatusCmd
                                 || command == kServerListCmd
                                 || command == kLogicalPoolList);
}

void StatusTool::PrintHelp(const std::string& cmd) {
    std::cout << "Example :" << std::endl;
    std::cout << "curve_ops_tool " << cmd;
    if (CommandNeedMds(cmd)) {
        std::cout << " [-mdsAddr=127.0.0.1:6666]"
                  << " [-confPath=/etc/curve/tools.conf]";
    }
    if (CommandNeedEtcd(cmd)) {
        std::cout << " [-etcdAddr=127.0.0.1:6666]"
                  << " [-confPath=/etc/curve/tools.conf]";
    }
    if (CommandNeedSnapshotClone(cmd)) {
        std::cout << " [-snapshotCloneAddr=127.0.0.1:5555]"
                  << " [-confPath=/etc/curve/tools.conf]";
    }
    if (cmd == kChunkserverListCmd) {
        std::cout << " [-offline] [-unhealthy] [-checkHealth=false]"
                  << " [-confPath=/etc/curve/tools.conf]"
                  << " [-checkCSAlive]";
    }
    if (cmd == kClientStatusCmd) {
        std::cout << " [-detail] [-confPath=/etc/curve/tools.conf]";
    }
    if (cmd == kClientListCmd) {
        std::cout << " [-listClientInRepo=false]"
                  << " [-confPath=/etc/curve/tools.conf]";
    }
    std::cout << std::endl;
}

int StatusTool::SpaceCmd() {
    SpaceInfo spaceInfo;
    int res = GetSpaceInfo(&spaceInfo);
    if (res != 0) {
        std::cout << "GetSpaceInfo fail!" << std::endl;
        return -1;
    }
    double physicalUsedRatio = 0;
    if (spaceInfo.totalChunkSize != 0) {
        physicalUsedRatio = static_cast<double>(spaceInfo.usedChunkSize) /
                                                    spaceInfo.totalChunkSize;
    }

    double logicalUsedRatio = 0;
    double logicalLeftRatio = 0;
    double canBeRecycledRatio = 0;
    double createdFileRatio = 0;
    if (spaceInfo.totalCapacity != 0) {
        logicalUsedRatio = static_cast<double>(spaceInfo.allocatedSize) /
                                                    spaceInfo.totalCapacity;
        logicalLeftRatio = static_cast<double>(
                        spaceInfo.totalCapacity - spaceInfo.allocatedSize) /
                                                    spaceInfo.totalCapacity;
        createdFileRatio = static_cast<double>(spaceInfo.currentFileSize) /
                                                    spaceInfo.totalCapacity;
    }
    if (spaceInfo.allocatedSize != 0) {
        canBeRecycledRatio = static_cast<double>(spaceInfo.recycleAllocSize) /
                                                        spaceInfo.allocatedSize;
    }
    std:: cout.setf(std::ios::fixed);
    std::cout << std::setprecision(2);
    std::cout << "Space info:" << std::endl;
    std::cout << "physical: total = "
              << spaceInfo.totalChunkSize / mds::kGB << "GB"
              << ", used = " << spaceInfo.usedChunkSize / mds::kGB
              << "GB(" << physicalUsedRatio * 100 << "%), left = "
              << (spaceInfo.totalChunkSize - spaceInfo.usedChunkSize) / mds::kGB
              << "GB(" << (1 - physicalUsedRatio) * 100 << "%)" << std::endl;
    std::cout << "logical: total = "
              << spaceInfo.totalCapacity / mds::kGB << "GB"
              << ", used = " << spaceInfo.allocatedSize / mds::kGB << "GB"
              << "(" << logicalUsedRatio * 100 << "%, can be recycled = "
              << spaceInfo.recycleAllocSize / mds::kGB << "GB("
              << canBeRecycledRatio * 100 << "%))"
              << ", left = "
              << (spaceInfo.totalCapacity - spaceInfo.allocatedSize) / mds::kGB
              << "GB(" << logicalLeftRatio * 100 << "%)"
              << ", created file size = "
              << spaceInfo.currentFileSize / mds::kGB
              << "GB(" << createdFileRatio * 100 << "%)" << std::endl;
    return 0;
}

int StatusTool::ChunkServerListCmd() {
    std::vector<ChunkServerInfo> chunkservers;
    int res = mdsClient_->ListChunkServersInCluster(&chunkservers);
    if (res != 0) {
        std::cout << "ListChunkserversInCluster fail!" << std::endl;
        return -1;
    }
    std::cout << "curve chunkserver list: " << std::endl;
    uint64_t total = 0;
    uint64_t online = 0;
    uint64_t offline = 0;
    uint64_t unstable = 0;
    for (auto& chunkserver : chunkservers) {
        auto csId = chunkserver.chunkserverid();
        double unhealthyRatio;
        if (FLAGS_checkCSAlive) {
            // 发RPC重置online状态
            std::string csAddr = chunkserver.hostip()
                        + ":" + std::to_string(chunkserver.port());
            bool isOnline = copysetCheckCore_->CheckChunkServerOnline(csAddr);
            if (isOnline) {
                chunkserver.set_onlinestate(OnlineState::ONLINE);
            } else {
                chunkserver.set_onlinestate(OnlineState::OFFLINE);
            }
        }
        if (chunkserver.onlinestate() != OnlineState::ONLINE) {
            if (chunkserver.onlinestate() == OnlineState::OFFLINE) {
                offline++;
            }

            if (chunkserver.onlinestate() == OnlineState::UNSTABLE) {
                unstable++;
            }
            unhealthyRatio = 1;
        } else {
            if (FLAGS_offline) {
                continue;
            }
            if (FLAGS_checkHealth) {
                copysetCheckCore_->CheckCopysetsOnChunkServer(csId);
                const auto& statistics =
                                copysetCheckCore_->GetCopysetStatistics();
                unhealthyRatio = statistics.unhealthyRatio;
                if (FLAGS_unhealthy && unhealthyRatio == 0) {
                    continue;
                }
            }
            online++;
        }
        total++;
        std::cout << "chunkServerID = " << csId
                  << ", diskType = " << chunkserver.disktype()
                  << ", hostIP = " << chunkserver.hostip()
                  << ", port = " << chunkserver.port()
                  << ", rwStatus = "
                  << ChunkServerStatus_Name(chunkserver.status())
                  << ", diskState = "
                  << DiskState_Name(chunkserver.diskstatus())
                  << ", onlineState = "
                  << OnlineState_Name(chunkserver.onlinestate())
                  << ", mountPoint = " << chunkserver.mountpoint()
                  << ", diskCapacity = " << chunkserver.diskcapacity()
                            / curve::mds::kGB << " GB"
                  << ", diskUsed = " << chunkserver.diskused()
                            / curve::mds::kGB << " GB";
        if (FLAGS_checkHealth) {
            std::cout <<  ", unhealthyCopysetRatio = "
                      << unhealthyRatio * 100 << "%";
        }
        if (chunkserver.has_externalip()) {
            std::cout << ", externalIP = " << chunkserver.externalip();
        }
        std::cout << std::endl;
    }
    std::cout << "total: " << total << ", online: " << online;
    if (!FLAGS_checkCSAlive) {
        std::cout <<", unstable: " << unstable;
    }
    std::cout << ", offline: " << offline << std::endl;
    return 0;
}

int StatusTool::ServerListCmd() {
    std::vector<ServerInfo> servers;
    int res = mdsClient_->ListServersInCluster(&servers);
    if (res != 0) {
        std::cout << "ListServersInCluster fail!" << std::endl;
        return -1;
    }
    std::cout << "curve server list: " << std::endl;
    uint64_t total = 0;
    for (auto& server : servers) {
        total++;
        std::cout << "serverID = " << server.serverid()
                  << ", hostName = " << server.hostname()
                  << ", internalIP = " << server.internalip()
                  << ", internalPort = " << server.internalport()
                  << ", externalIp = " << server.externalip()
                  << ", externalPort = " << server.externalport()
                  << ", zoneID = " << server.zoneid()
                  << ", poolID = " << server.physicalpoolid() << std::endl;
    }
    std::cout << "total: " << total << std::endl;
    return 0;
}

int StatusTool::LogicalPoolListCmd() {
    std::vector<LogicalPoolInfo> lgPools;
    int res = mdsClient_->ListLogicalPoolsInCluster(&lgPools);
    if (res != 0) {
        std::cout << "ListLogicalPoolsInCluster fail!" << std::endl;
        return -1;
    }
    std::cout << "curve logical pool list: " << std::endl;
    uint64_t total = 0;
    uint64_t allocSize;
    AllocMap allocMap;
    res = mdsClient_->GetAllocatedSize(curve::mds::RECYCLEBINDIR,
                                       &allocSize, &allocMap);
    if (res != 0) {
        std::cout << "GetAllocatedSize of recycle bin fail!" << std::endl;
        return -1;
    }
    for (auto& lgPool : lgPools) {
        total++;
        std::string poolName = lgPool.logicalpoolname();
        uint64_t totalSize;
        std::string metricName = GetPoolLogicalCapacityName(poolName);
        res = mdsClient_->GetMetric(metricName, &totalSize);
        if (res != 0) {
            std::cout << "Get logical capacity from mds fail!" << std::endl;
            return -1;
        }
        uint64_t usedSize;
        metricName = GetPoolLogicalAllocName(poolName);
        res = mdsClient_->GetMetric(metricName, &usedSize);
        if (res != 0) {
            std::cout << "Get logical alloc size from mds fail!" << std::endl;
            return -1;
        }
        double usedRatio = 0;
        if (total != 0) {
            usedRatio = static_cast<double>(usedSize) / totalSize;
        }
        uint64_t canBeRecycle = allocMap[lgPool.logicalpoolid()];
        double recycleRatio = 0;
        if (usedSize != 0) {
            recycleRatio = static_cast<double>(canBeRecycle) / usedSize;
        }
        std::cout << "id = " << lgPool.logicalpoolid()
                  << ", name = " << lgPool.logicalpoolname()
                  << ", physicalPoolID = " << lgPool.physicalpoolid()
                  << ", type = "
                  << curve::mds::topology::LogicalPoolType_Name(lgPool.type())
                  << ", total space = " << totalSize / curve::mds::kGB << "GB"
                  << ", used space = " << usedSize / curve::mds::kGB << "GB"
                  << "(" << usedRatio * 100 << "%, can be recycled = "
                  << canBeRecycle / curve::mds::kGB  << "GB"
                  << "(" << recycleRatio * 100 << "%))" << ", left space = "
                  << (totalSize - usedSize) / curve::mds::kGB
                  << "GB(" << (1 - usedRatio) * 100 << "%)" << std::endl;
    }
    std::cout << "total: " << total << std::endl;
    return 0;
}

int StatusTool::StatusCmd() {
    int res = PrintClusterStatus();
    bool success = true;
    if (res != 0) {
        success = false;
    }
    std::cout << std::endl;
    res = PrintClientStatus();
    if (res != 0) {
        success = false;
    }
    std::cout << std::endl;
    res = PrintMdsStatus();
    if (res != 0) {
        success = false;
    }
    std::cout << std::endl;
    res = PrintEtcdStatus();
    if (res != 0) {
        success = false;
    }
    std::cout << std::endl;
    res = PrintSnapshotCloneStatus();
    if (res != 0) {
        success = false;
    }
    std::cout << std::endl;
    res = PrintChunkserverStatus();
    if (res != 0) {
        success = false;
    }
    if (success) {
        return 0;
    } else {
        return -1;
    }
}

int StatusTool::ChunkServerStatusCmd() {
    return PrintChunkserverStatus(false);
}

int StatusTool::PrintClusterStatus() {
    int ret = 0;
    std::cout << "Cluster status:" << std::endl;
    bool healthy = IsClusterHeatlhy();
    if (healthy) {
        std::cout << "cluster is healthy" << std::endl;
    } else {
        std::cout << "cluster is not healthy" << std::endl;
        ret = -1;
    }
    const auto& statistics = copysetCheckCore_->GetCopysetStatistics();
    std::cout << "total copysets: " << statistics.totalNum
              << ", unhealthy copysets: " << statistics.unhealthyNum
              << ", unhealthy_ratio: "
              << statistics.unhealthyRatio * 100 << "%" << std::endl;
    std::vector<PhysicalPoolInfo> phyPools;
    std::vector<LogicalPoolInfo> lgPools;
    int res = GetPoolsInCluster(&phyPools, &lgPools);
    if (res != 0) {
        std::cout << "GetPoolsInCluster fail!" << std::endl;
        ret = -1;
    }
    std::cout << "physical pool number: " << phyPools.size()
              << ", logical pool number: " << lgPools.size() << std::endl;
    res = SpaceCmd();
    if (res != 0) {
        ret = -1;
    }
    return ret;
}

bool StatusTool::IsClusterHeatlhy() {
    bool ret = true;
    // 1、检查copyset健康状态
    int res = copysetCheckCore_->CheckCopysetsInCluster();
    if (res != 0) {
        std::cout << "Copysets are not healthy!" << std::endl;
        ret = false;
    }

    // 2、检查mds状态
    if (!CheckServiceHealthy(ServiceName::kMds)) {
        ret = false;
    }

    // 3、检查etcd在线状态
    if (!CheckServiceHealthy(ServiceName::kEtcd)) {
        ret = false;
    }

    // 4、检查snapshot clone server状态
    if (!CheckServiceHealthy(ServiceName::kSnapshotCloneServer)) {
        ret = false;
    }

    return ret;
}

bool StatusTool::CheckServiceHealthy(const ServiceName& name) {
    std::vector<std::string> leaderVec;
    std::map<std::string, bool> onlineStatus;
    switch (name) {
        case ServiceName::kMds: {
            leaderVec = mdsClient_->GetCurrentMds();
            mdsClient_->GetMdsOnlineStatus(&onlineStatus);
            break;
        }
        case ServiceName::kEtcd: {
            int res = etcdClient_->GetEtcdClusterStatus(&leaderVec,
                                                        &onlineStatus);
            if (res != 0) {
                std:: cout << "GetEtcdClusterStatus fail!" << std::endl;
                return false;
            }
            break;
        }
        case ServiceName::kSnapshotCloneServer: {
            leaderVec = snapshotClient_->GetActiveAddrs();
            snapshotClient_->GetOnlineStatus(&onlineStatus);
            break;
        }
        default: {
            std::cout << "Unknown service" << std::endl;
            return false;
        }
    }
    bool ret = true;
    if (leaderVec.empty()) {
        std::cout << "No " << ToString(name) << " is active" << std::endl;
        ret = false;
    } else if (leaderVec.size() != 1) {
        std::cout << "More than one " << ToString(name) << " is active"
                  << std::endl;
        ret = false;
    }
    for (const auto& item : onlineStatus) {
        if (!item.second) {
            std::cout << ToString(name) << " " << item.first << " is offline"
                      << std::endl;
            ret = false;
        }
    }
    return ret;
}

void StatusTool::PrintOnlineStatus(const std::string& name,
                    const std::map<std::string, bool>& onlineStatus) {
    std::vector<std::string> online;
    std::vector<std::string> offline;
    for (const auto& item : onlineStatus) {
        if (item.second) {
            online.emplace_back(item.first);
        } else {
            offline.emplace_back(item.first);
        }
    }
    std::cout << "online " << name << " list: ";
    for (uint64_t i = 0; i < online.size(); ++i) {
        if (i != 0) {
            std::cout << ", ";
        }
        std::cout << online[i];
    }
    std::cout << std::endl;

    std::cout << "offline " << name << " list: ";
    for (uint64_t i = 0; i < offline.size(); ++i) {
        if (i != 0) {
            std::cout << ", ";
        }
        std::cout << offline[i];
    }
    std::cout << std::endl;
}

int StatusTool::PrintMdsStatus() {
    std::cout << "MDS status:" << std::endl;
    std::string version;
    std::vector<std::string> failedList;
    int res = versionTool_->GetAndCheckMdsVersion(&version, &failedList);
    int ret = 0;
    if (res != 0) {
        std::cout << "GetAndCheckMdsVersion fail" << std::endl;
        ret = -1;
    } else {
        std::cout << "version: " << version << std::endl;
        if (!failedList.empty()) {
            versionTool_->PrintFailedList(failedList);
            ret = -1;
        }
    }
    std::vector<std::string> mdsAddrs = mdsClient_->GetCurrentMds();
    std::cout << "current MDS: " << mdsAddrs << std::endl;
    std::map<std::string, bool> onlineStatus;
    mdsClient_->GetMdsOnlineStatus(&onlineStatus);
    if (res != 0) {
        std::cout << "GetMdsOnlineStatus fail!" << std::endl;
        ret = -1;
    } else {
        PrintOnlineStatus("mds", onlineStatus);
    }
    return ret;
}

int StatusTool::PrintEtcdStatus() {
    std::cout << "Etcd status:" << std::endl;
    std::string version;
    std::vector<std::string> failedList;
    int res = etcdClient_->GetAndCheckEtcdVersion(&version, &failedList);
    int ret = 0;
    if (res != 0) {
        std::cout << "GetAndCheckEtcdVersion fail" << std::endl;
        ret = -1;
    } else {
        std::cout << "version: " << version << std::endl;
        if (!failedList.empty()) {
            VersionTool::PrintFailedList(failedList);
            ret = -1;
        }
    }
    std::vector<std::string> leaderAddrVec;
    std::map<std::string, bool> onlineStatus;
    res = etcdClient_->GetEtcdClusterStatus(&leaderAddrVec, &onlineStatus);
    if (res != 0) {
        std::cout << "GetEtcdClusterStatus fail!" << std::endl;
        return -1;
    }
    std::cout << "current etcd: " << leaderAddrVec << std::endl;
    PrintOnlineStatus("etcd", onlineStatus);
    return ret;
}

int StatusTool::PrintSnapshotCloneStatus() {
    std::cout << "SnapshotCloneServer status:" << std::endl;
    std::string version;
    std::vector<std::string> failedList;
    int res = versionTool_->GetAndCheckSnapshotCloneVersion(&version,
                                                            &failedList);
    int ret = 0;
    if (res != 0) {
        std::cout << "GetAndCheckSnapshotCloneVersion fail" << std::endl;
        ret = -1;
    } else {
        std::cout << "version: " << version << std::endl;
        if (!failedList.empty()) {
            versionTool_->PrintFailedList(failedList);
            ret = -1;
        }
    }
    std::vector<std::string> activeAddrs = snapshotClient_->GetActiveAddrs();
    std::map<std::string, bool> onlineStatus;
    snapshotClient_->GetOnlineStatus(&onlineStatus);
    std::cout << "current snapshot-clone-server: " << activeAddrs << std::endl;
    PrintOnlineStatus("snapshot-clone-server", onlineStatus);
    return ret;
}

int StatusTool::PrintClientStatus() {
    std::cout << "Client status: " << std::endl;
    ClientVersionMapType versionMap;
    int res = versionTool_->GetClientVersion(&versionMap);
    if (res != 0) {
        std::cout << "GetClientVersion fail" << std::endl;
        return -1;
    }
    for (const auto& item : versionMap) {
        std::cout << item.first << ": ";
        bool first = true;
        for (const auto& item2 : item.second) {
            if (!first) {
                std::cout << ", ";
            }
            std::cout << "version-" <<  item2.first << ": "
                      << item2.second.size();
            first = false;
        }
        std::cout << std::endl;
        if (FLAGS_detail) {
            std::cout << "version map: " << std::endl;
            versionTool_->PrintVersionMap(item.second);
        }
    }
    return 0;
}

int StatusTool::ClientListCmd() {
    std::vector<std::string> clientAddrs;
    int res = mdsClient_->ListClient(&clientAddrs, FLAGS_listClientInRepo);
    if (res != 0) {
        std::cout << "ListClient from mds fail!" << std::endl;
        return -1;
    }
    for (const auto& addr : clientAddrs) {
        std::cout << addr << std::endl;
    }
    return 0;
}

int StatusTool::PrintChunkserverStatus(bool checkLeftSize) {
    std::cout << "ChunkServer status:" << std::endl;
    std::string version;
    std::vector<std::string> failedList;
    int res = versionTool_->GetAndCheckChunkServerVersion(&version,
                                                          &failedList);
    int ret = 0;
    if (res != 0) {
        std::cout << "GetAndCheckChunkserverVersion fail" << std::endl;
        ret = -1;
    } else {
        std::cout << "version: " << version << std::endl;
        if (!failedList.empty()) {
            versionTool_->PrintFailedList(failedList);
            ret = -1;
        }
    }
    std::map<PoolIdType, std::vector<ChunkServerInfo>> poolChunkservers;
    res = mdsClient_->ListChunkServersInCluster(&poolChunkservers);
    if (res != 0) {
        std::cout << "ListChunkServersInCluster fail!" << std::endl;
        return -1;
    }
    uint64_t total = 0;
    uint64_t online = 0;
    uint64_t offline = 0;
    std::map<PoolIdType, std::vector<uint64_t>> poolChunkLeftSize;
    std::map<PoolIdType, std::vector<uint64_t>> poolWalSegmentLeftSize;
    std::vector<ChunkServerIdType> offlineCs;
    // 获取chunkserver的online状态
    for (const auto& poolChunkserver : poolChunkservers) {
        std::vector<uint64_t> chunkLeftSize;
        std::vector<uint64_t> walSegmentLeftSize;
        for (const auto& chunkserver : poolChunkserver.second) {
            total++;
            std::string csAddr = chunkserver.hostip()
                            + ":" + std::to_string(chunkserver.port());
            if (copysetCheckCore_->CheckChunkServerOnline(csAddr)) {
                online++;
            } else {
                offline++;
                offlineCs.emplace_back(chunkserver.chunkserverid());
            }
            if (!checkLeftSize) {
                continue;
            }
            std::string metricName = GetCSLeftChunkName(csAddr);
            uint64_t chunkNum;
            MetricRet res = metricClient_->GetMetricUint(csAddr,
                                                        metricName, &chunkNum);
            if (res != MetricRet::kOK) {
                std::cout << "Get left chunk size of chunkserver " << csAddr
                        << " fail!" << std::endl;
                ret = -1;
                continue;
            }
            uint64_t size = chunkNum * FLAGS_chunkSize;
            chunkLeftSize.emplace_back(size / mds::kGB);
            // walfilepool left size
            metricName = GetCSLeftWalSegmentName(csAddr);
            uint64_t walSegmentNum;
            res = metricClient_->GetMetricUint(csAddr, metricName,
                                                    &walSegmentNum);
            if (res != MetricRet::kOK) {
                std::cout << "Get left wal segment size of chunkserver "
                          << csAddr << " fail!" << std::endl;
                ret = -1;
                continue;
            }
            size = walSegmentNum * FLAGS_walSegmentSize;
            walSegmentLeftSize.emplace_back(size / mds::kGB);
        }
        poolChunkLeftSize.emplace(poolChunkserver.first, chunkLeftSize);
        poolWalSegmentLeftSize.emplace(poolChunkserver.first,
                                                    walSegmentLeftSize);
    }
    // 获取offline chunkserver的恢复状态
    std::vector<ChunkServerIdType> offlineRecover;
    if (offlineCs.size() > 0) {
        // 获取offline中的chunkserver恢复状态
        std::map<ChunkServerIdType, bool> statusMap;
        int res = mdsClient_->QueryChunkServerRecoverStatus(
            offlineCs, &statusMap);
        if (res != 0) {
            std::cout << "query offlinne chunkserver recover status fail";
        } else {
            // 区分正在恢复的和未恢复的
            for (auto it = statusMap.begin(); it != statusMap.end(); ++it) {
                if (it->second) {
                    offlineRecover.emplace_back(it->first);
                }
            }
        }
    }

    std::cout << "chunkserver: total num = " << total
            << ", online = " << online
            << ", offline = " << offline
            << "(recoveringout = " << offlineRecover.size()
            << ", chunkserverlist: [";

    int i = 0;
    for (ChunkServerIdType csId :  offlineRecover) {
        i++;
        if (i == offlineRecover.size()) {
            std::cout << csId;
        } else {
            std::cout << csId << ", ";
        }
    }
    std::cout << "])" << std::endl;
    if (!checkLeftSize) {
        return ret;
    }

    PrintCsLeftSizeStatistics("chunkfilepool", poolChunkLeftSize);
    PrintCsLeftSizeStatistics("walfilepool", poolWalSegmentLeftSize);
    return ret;
}

void StatusTool::PrintCsLeftSizeStatistics(const std::string& name,
                                    const std::map<PoolIdType,
                                    std::vector<uint64_t>>& poolLeftSize) {
    if (poolLeftSize.empty()) {
        std::cout << "No " << name << " left size found!" << std::endl;
        return;
    }
    for (const auto& leftSize : poolLeftSize) {
        uint64_t min = leftSize.second[0];
        uint64_t max = leftSize.second[0];
        double sum = 0;
        for (const auto& size : leftSize.second) {
            sum += size;
            if (size < min) {
                min = size;
            }
            if (size > max) {
                max = size;
            }
        }
        uint64_t range = max - min;
        double avg = sum / leftSize.second.size();
        sum = 0;
        for (const auto& size : leftSize.second) {
            sum += (size - avg) * (size - avg);
        }

        double var = sum / leftSize.second.size();
        std:: cout.setf(std::ios::fixed);
        std::cout<< std::setprecision(2);
        std::cout<< "pool" << leftSize.first << " " << name;
        std::cout << " left size: min = " << min << "GB"
                    << ", max = " << max << "GB"
                    << ", average = " << avg << "GB"
                    << ", range = " << range << "GB"
                    << ", variance = " << var << std::endl;
    }
}

int StatusTool::GetPoolsInCluster(std::vector<PhysicalPoolInfo>* phyPools,
                          std::vector<LogicalPoolInfo>* lgPools) {
    int res = mdsClient_->ListPhysicalPoolsInCluster(phyPools);
    if (res != 0) {
        std::cout << "ListPhysicalPoolsInCluster fail!" << std::endl;
        return -1;
    }
    for (const auto& phyPool : *phyPools) {
        int res = mdsClient_->ListLogicalPoolsInPhysicalPool(
                    phyPool.physicalpoolid(), lgPools) != 0;
        if (res != 0) {
            std::cout << "ListLogicalPoolsInPhysicalPool fail!" << std::endl;
            return -1;
        }
    }
    return 0;
}

int StatusTool::GetSpaceInfo(SpaceInfo* spaceInfo) {
    std::vector<LogicalPoolInfo> lgPools;
    int res = mdsClient_->ListLogicalPoolsInCluster(&lgPools);
    if (res != 0) {
        std::cout << "ListLogicalPoolsInCluster fail!" << std::endl;
        return -1;
    }
    res = mdsClient_->GetFileSize(curve::mds::ROOTFILENAME,
                                  &spaceInfo->currentFileSize);
    if (res != 0) {
        std::cout << "Get root directory file size from mds fail!" << std::endl;
        return -1;
    }
    // 从metric获取space信息
    for (const auto& lgPool : lgPools) {
        std::string poolName = lgPool.logicalpoolname();
        std::string metricName = GetPoolTotalChunkSizeName(poolName);
        uint64_t size;
        int res = mdsClient_->GetMetric(metricName, &size);
        if (res != 0) {
            std::cout << "Get total chunk byte from mds fail!" << std::endl;
            return -1;
        }
        spaceInfo->totalChunkSize += size;
        metricName = GetPoolUsedChunkSizeName(poolName);
        res = mdsClient_->GetMetric(metricName, &size);
        if (res != 0) {
            std::cout << "Get used chunk byte from mds fail!" << std::endl;
            return -1;
        }
        spaceInfo->usedChunkSize += size;
        metricName = GetPoolLogicalCapacityName(poolName);
        res = mdsClient_->GetMetric(metricName, &size);
        if (res != 0) {
            std::cout << "Get logical capacity from mds fail!" << std::endl;
            return -1;
        }
        spaceInfo->totalCapacity += size;
        metricName = GetPoolLogicalAllocName(poolName);
        res = mdsClient_->GetMetric(metricName, &size);
        if (res != 0) {
            std::cout << "Get logical alloc size from mds fail!" << std::endl;
            return -1;
        }
        spaceInfo->allocatedSize += size;
    }
    // 获取RecycleBin的分配大小
    res = mdsClient_->GetAllocatedSize(curve::mds::RECYCLEBINDIR,
                                       &spaceInfo->recycleAllocSize);
    if (res != 0) {
        std::cout << "GetAllocatedSize of RecycleBin fail!" << std::endl;
        return -1;
    }
    return 0;
}

int StatusTool::RunCommand(const std::string &cmd) {
    if (Init(cmd) != 0) {
        std::cout << "Init StatusTool failed" << std::endl;
        return -1;
    }
    if (cmd == kSpaceCmd) {
        return SpaceCmd();
    } else if (cmd == kStatusCmd) {
        return StatusCmd();
    } else if (cmd == kChunkserverListCmd) {
        return ChunkServerListCmd();
    } else if (cmd == kServerListCmd) {
        return ServerListCmd();
    } else if (cmd == kLogicalPoolList) {
        return LogicalPoolListCmd();
    } else if (cmd == kChunkserverStatusCmd) {
        return ChunkServerStatusCmd();
    } else if (cmd == kMdsStatusCmd) {
        return PrintMdsStatus();
    } else if (cmd == kEtcdStatusCmd) {
        return PrintEtcdStatus();
    } else if (cmd == kClientStatusCmd) {
        return PrintClientStatus();
    } else if (cmd == kSnapshotCloneStatusCmd) {
        return PrintSnapshotCloneStatus();
    } else if (cmd == kClusterStatusCmd) {
        return PrintClusterStatus();
    } else if (cmd == kClientListCmd) {
        return ClientListCmd();
    } else {
        std::cout << "Command not supported!" << std::endl;
        return -1;
    }

    return 0;
}
}  // namespace tool
}  // namespace curve

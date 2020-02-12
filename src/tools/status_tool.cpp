/*
 * Project: curve
 * Created Date: 2019-12-03
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#include "src/tools/status_tool.h"
#include <utility>

DEFINE_bool(offline, false, "if true, only list offline chunskervers");
DEFINE_bool(unhealthy, false, "if true, only list chunkserver that unhealthy "
                              "ratio greater than 0");
DEFINE_bool(checkHealth, true, "if true, it will check the health "
                                "state of chunkserver in chunkserver-list");
DECLARE_string(mdsAddr);
DECLARE_string(etcdAddr);
DECLARE_string(mdsDummyPort);

namespace curve {
namespace tool {

int StatusTool::Init(const std::string& command) {
    if (CommandNeedMds(command) && !mdsInited_) {
        if (mdsClient_->Init(FLAGS_mdsAddr, FLAGS_mdsDummyPort) != 0) {
            std::cout << "Init mdsClient failed!" << std::endl;
            return -1;
        }
        if (nameSpaceToolCore_->Init(FLAGS_mdsAddr) != 0) {
            std::cout << "Init nameSpaceToolCore failed!" << std::endl;
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
    return 0;
}

bool StatusTool::CommandNeedEtcd(const std::string& command) {
    return (command == kEtcdStatusCmd || command == kStatusCmd);
}

bool StatusTool::CommandNeedMds(const std::string& command) {
    return (command != kEtcdStatusCmd);
}

bool StatusTool::SupportCommand(const std::string& command) {
    return (command == kSpaceCmd || command == kStatusCmd
                                 || command == kChunkserverListCmd
                                 || command == kChunkserverStatusCmd
                                 || command == kMdsStatusCmd
                                 || command == kEtcdStatusCmd);
}

void StatusTool::PrintHelp(const std::string& cmd) {
    std::cout << "Example :" << std::endl;
    std::cout << "curve_ops_tool " << cmd;
    if (CommandNeedMds(cmd)) {
        std::cout << " -mdsAddr=127.0.0.1:6666";
    }
    if (CommandNeedEtcd(cmd)) {
        std::cout << " -etcdAddr=127.0.0.1:6666";
    }
    if (cmd == kChunkserverListCmd) {
        std::cout << " [-offline] [-unhealthy] [-checkHealth=false]";
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
    std::cout << "total space = " << spaceInfo.total / mds::kGB << "GB"
              << ", logical used = " << spaceInfo.logicalUsed / mds::kGB << "GB"
              << ", physical used = " << spaceInfo.physicalUsed / mds::kGB
              << "GB"
              << ", can be recycled = " << spaceInfo.canBeRecycled / mds::kGB
              << "GB"
              << std::endl;
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
    for (const auto& chunkserver : chunkservers) {
        auto csId = chunkserver.chunkserverid();
        double unhealthyRatio;
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
        std::cout << std::endl;
    }
    std::cout << "total: " << total << ", online: " << online
        <<", unstable: " << unstable << ", offline: " << offline << std::endl;
    return 0;
}

int StatusTool::StatusCmd() {
    int res = PrintClusterStatus();
    bool success = true;
    if (res != 0) {
        std::cout << "PrintClusterStatus fail!" << std::endl;
        success = false;
    }
    std::cout << std::endl;
    res = PrintMdsStatus();
    if (res != 0) {
        std::cout << "PrintMdsStatus fail!" << std::endl;
        success = false;
    }
    std::cout << std::endl;
    res = PrintEtcdStatus();
    if (res != 0) {
        std::cout << "PrintEtcdStatus fail!" << std::endl;
        success = false;
    }
    std::cout << std::endl;
    res = PrintChunkserverStatus();
    if (res != 0) {
        std::cout << "PrintChunkserverStatus fail!" << std::endl;
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
    std::cout << "Cluster status:" << std::endl;
    int res = copysetCheckCore_->CheckCopysetsInCluster();
    if (res == 0) {
        std::cout << "cluster is healthy!" << std::endl;
    } else {
        std::cout << "cluster is not healthy!" << std::endl;
    }
    const auto& statistics = copysetCheckCore_->GetCopysetStatistics();
    std::cout << "total copysets: " << statistics.totalNum
              << ", unhealthy copysets: " << statistics.unhealthyNum
              << ", unhealthy_ratio: "
              << statistics.unhealthyRatio * 100 << "%" << std::endl;
    std::vector<PhysicalPoolInfo> phyPools;
    std::vector<LogicalPoolInfo> lgPools;
    res = GetPoolsInCluster(&phyPools, &lgPools);
    if (res != 0) {
        std::cout << "GetPoolsInCluster fail!" << std::endl;
        return -1;
    }
    std::cout << "physical pool number: " << phyPools.size()
              << ", logical pool number: " << lgPools.size() << std::endl;
    return SpaceCmd();
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
    std::cout << "current MDS: " << mdsClient_->GetCurrentMds() << std::endl;
    std::map<std::string, bool> onlineStatus;
    int res = mdsClient_->GetMdsOnlineStatus(&onlineStatus);
    if (res != 0) {
        std::cout << "GetMdsOnlineStatus fail!" << std::endl;
        return -1;
    }
    PrintOnlineStatus("mds", onlineStatus);
    return 0;
}

int StatusTool::PrintEtcdStatus() {
    std::cout << "Etcd status:" << std::endl;
    std::string leaderAddr;
    std::map<std::string, bool> onlineStatus;
    int res = etcdClient_->GetEtcdClusterStatus(&leaderAddr, &onlineStatus);
    if (res != 0) {
        std::cout << "GetEtcdClusterStatus fail!" << std::endl;
        return -1;
    }
    std::cout << "current etcd: " << leaderAddr << std::endl;
    PrintOnlineStatus("etcd", onlineStatus);
    return 0;
}

int StatusTool::PrintChunkserverStatus(bool checkLeftSize) {
    std::cout << "ChunkServer status:" << std::endl;
    std::vector<ChunkServerInfo> chunkservers;
    int res = mdsClient_->ListChunkServersInCluster(&chunkservers);
    if (res != 0) {
        std::cout << "ListChunkServersInCluster fail!" << std::endl;
        return -1;
    }
    uint64_t total = 0;
    uint64_t online = 0;
    uint64_t offline = 0;
    uint64_t unstable = 0;
    std::map<uint64_t, int> leftSizeNum;
    for (const auto& chunkserver : chunkservers) {
        if (chunkserver.onlinestate() == OnlineState::ONLINE) {
            online++;
        } else if (chunkserver.onlinestate() == OnlineState::UNSTABLE) {
            unstable++;
        } else {
            offline++;
        }
        total++;
        if (!checkLeftSize) {
            continue;
        }
        auto csId = chunkserver.chunkserverid();
        std::string metricName = GetCSLeftBytesName(csId);
        uint64_t size;
        int res = mdsClient_->GetMetric(metricName, &size);
        if (res != 0) {
            std::cout << "Get left chunk size of chunkserver " << csId
                      << " fail!" << std::endl;
            return -1;
        }
        if (leftSizeNum.count(size) == 0) {
            leftSizeNum[size] = 1;
        } else {
            leftSizeNum[size]++;
        }
    }
    std::cout << "chunkserver: total num = " << total
            << ", online = " << online
            << ", unstable = " << unstable
            << ", offline = " << offline << std::endl;
    if (!checkLeftSize) {
        return 0;
    }
    if (leftSizeNum.empty()) {
        std::cout << "No chunkserver left chunk size found!" << std::endl;
        return -1;
    }
    auto minPair = leftSizeNum.begin();
    std::cout << "minimal left size: " << minPair->first / mds::kGB << "GB"
              << ", chunkserver num: " << minPair->second << std::endl;
    return 0;
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
    std::vector<PhysicalPoolInfo> phyPools;
    std::vector<LogicalPoolInfo> lgPools;
    int res = GetPoolsInCluster(&phyPools, &lgPools);
    if (res != 0) {
        std::cout << "GetPoolsInCluster fail!" << std::endl;
        return -1;
    }
    // 从metric获取total，logicalUsed和physicalUsed
    for (const auto& lgPool : lgPools) {
        std::string poolName = lgPool.logicalpoolname();
        std::string metricName = GetPoolTotalBytesName(poolName);
        uint64_t size;
        int res = mdsClient_->GetMetric(metricName, &size);
        if (res != 0) {
            std::cout << "Get total space from mds fail!" << std::endl;
            return -1;
        }
        spaceInfo->total += size;
        metricName = GetPoolDiskAllocName(poolName);
        res = mdsClient_->GetMetric(metricName, &size);
        if (res != 0) {
            std::cout << "Get logical used space from mds fail!" << std::endl;
            return -1;
        }
        spaceInfo->logicalUsed += size;
        metricName = GetPoolUsedBytesName(poolName);
        res = mdsClient_->GetMetric(metricName, &size);
        if (res != 0) {
            std::cout << "Get physical used space from mds fail!" << std::endl;
            return -1;
        }
        spaceInfo->physicalUsed += size;
    }
    // 通过NameSpace工具获取RecycleBin的大小
    res = nameSpaceToolCore_->GetAllocatedSize(curve::mds::RECYCLEBINDIR,
                                         &spaceInfo->canBeRecycled);
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
    } else if (cmd == kChunkserverStatusCmd) {
        return ChunkServerStatusCmd();
    } else if (cmd == kMdsStatusCmd) {
        return PrintMdsStatus();
    } else if (cmd == kEtcdStatusCmd) {
        return PrintEtcdStatus();
    } else {
        std::cout << "Command not supported!" << std::endl;
        return -1;
    }

    return 0;
}
}  // namespace tool
}  // namespace curve

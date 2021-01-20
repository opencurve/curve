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
#include "src/tools/copyset_check.h"
#include "src/tools/common.h"
#include "src/tools/metric_name.h"

DEFINE_bool(detail, false, "list the copyset detail or not");
DEFINE_uint32(chunkserverId, 0, "chunkserver id");
DEFINE_string(chunkserverAddr, "", "if specified, chunkserverId is not required");  // NOLINT
DEFINE_uint32(serverId, 0, "server id");
DEFINE_string(serverIp, "", "server ip");
DEFINE_string(opName, curve::tool::kTotalOpName, "operator name");
DECLARE_string(mdsAddr);
DEFINE_uint64(opIntervalExceptLeader, 5, "Operator generation interval other "
                                         "than transfer leader");
DEFINE_uint64(leaderOpInterval, 30,
                        "tranfer leader operator generation interval");

namespace curve {
namespace tool {
#define CHECK_ONLY_ONE_SHOULD_BE_SPECIFIED(flagname1, flagname2)             \
    do {                                                                     \
        if ((FLAGS_ ## flagname1).empty() && (FLAGS_ ## flagname2) == 0) {   \
            std::cout << # flagname1 << " OR " << # flagname2                \
                      " should be secified!" << std::endl;                   \
            return -1;                                                       \
        }                                                                    \
        if (!(FLAGS_ ## flagname1).empty() && (FLAGS_ ## flagname2) != 0) {  \
            std::cout << "Only one of " # flagname1 << " OR " << # flagname2 \
                      " should be secified!" << std::endl;                   \
            return -1;                                                       \
        }                                                                    \
    } while (0);                                                             \

bool CopysetCheck::SupportCommand(const std::string& command) {
    return (command == kCheckCopysetCmd || command == kCheckChunnkServerCmd
            || command == kCheckServerCmd || command == kCopysetsStatusCmd
            || command == kCheckOperatorCmd
            || command == kListMayBrokenVolumes);
}

int CopysetCheck::Init() {
    if (!inited_) {
        int res = core_->Init(FLAGS_mdsAddr);
        if (res != 0) {
            std::cout << "Init copysetCheckCore fail!" << std::endl;
            return -1;
        }
        inited_ = true;
    }
    return 0;
}

int CopysetCheck::RunCommand(const std::string& command) {
    if (Init() != 0) {
        std::cout << "Init CopysetCheck failed" << std::endl;
        return -1;
    }
    if (command == kCheckCopysetCmd) {
        // 检查某个copyset的状态
        if (FLAGS_logicalPoolId == 0 || FLAGS_copysetId == 0) {
            std::cout << "logicalPoolId AND copysetId should be specified!"
                      << std::endl;
            return -1;
        }
        return CheckCopyset();
    } else if (command == kCheckChunnkServerCmd) {
        // 检查某个chunkserver上的所有copyset
        CHECK_ONLY_ONE_SHOULD_BE_SPECIFIED(chunkserverAddr, chunkserverId);
        return CheckChunkServer();
    } else if (command == kCheckServerCmd) {
        CHECK_ONLY_ONE_SHOULD_BE_SPECIFIED(serverIp, serverId);
        return CheckServer();
    } else if (command == kCopysetsStatusCmd) {
        return CheckCopysetsInCluster();
    } else if (command == kCheckOperatorCmd) {
        if (!SupportOpName(FLAGS_opName)) {
            std::cout << "only support opName: ";
            PrintSupportOpName();
            return -1;
        }
        return CheckOperator(FLAGS_opName);
    } else if (command == kListMayBrokenVolumes) {
        return PrintMayBrokenVolumes();
    } else {
        PrintHelp(command);
        return -1;
    }
}

int CopysetCheck::CheckCopyset() {
    int res = core_->CheckOneCopyset(FLAGS_logicalPoolId, FLAGS_copysetId);
    if (res == 0) {
        std::cout << "Copyset is healthy!" << std::endl;
    } else {
        std::cout << "Copyset is not healthy!" << std::endl;
    }
    if (FLAGS_detail) {
        std::cout << core_->GetCopysetDetail() << std::endl;
        PrintExcepChunkservers();
    }
    return res;
}

int CopysetCheck::CheckChunkServer() {
    int res = 0;
    if (FLAGS_chunkserverId > 0) {
        res = core_->CheckCopysetsOnChunkServer(FLAGS_chunkserverId);
    } else {
        res = core_->CheckCopysetsOnChunkServer(FLAGS_chunkserverAddr);
    }
    if (res == 0) {
        std::cout << "ChunkServer is healthy!" << std::endl;
    } else {
        std::cout << "ChunkServer is not healthy!" << std::endl;
    }
    PrintStatistic();
    if (FLAGS_detail) {
        PrintDetail();
    }
    return res;
}

int CopysetCheck::CheckServer() {
    std::vector<std::string> unhealthyCs;
    int res = 0;
    if (FLAGS_serverId > 0) {
        res = core_->CheckCopysetsOnServer(FLAGS_serverId, &unhealthyCs);
    } else {
        res = core_->CheckCopysetsOnServer(FLAGS_serverIp, &unhealthyCs);
    }
    if (res == 0) {
        std::cout << "Server is healthy!" << std::endl;
    } else {
        std::cout << "Server is not healthy!" << std::endl;
    }
    PrintStatistic();
    if (FLAGS_detail) {
        PrintDetail();
        std::ostream_iterator<std::string> out(std::cout, ", ");
        std::cout << "unhealthy chunkserver list (total: "
                  << unhealthyCs.size() <<"): {";
        std::copy(unhealthyCs.begin(), unhealthyCs.end(), out);
        std::cout << "}" << std::endl;
    }
    return res;
}

int CopysetCheck::CheckCopysetsInCluster() {
    int res = core_->CheckCopysetsInCluster();
    if (res == 0) {
        std::cout << "Copysets are healthy!" << std::endl;
    } else {
        std::cout << "Copysets not healthy!" << std::endl;
    }
    PrintStatistic();
    if (FLAGS_detail) {
        PrintDetail();
    }
    return res;
}

int CopysetCheck::CheckOperator(const std::string& opName) {
    int res;
    if (opName == kTransferOpName || opName == kTotalOpName) {
        res = core_->CheckOperator(opName, FLAGS_leaderOpInterval);
    } else {
        res = core_->CheckOperator(opName, FLAGS_opIntervalExceptLeader);
    }
     if (res < 0) {
        std::cout << "Check operator fail!" << std::endl;
    } else {
        std::cout << "Operator num is "
                  << res << std::endl;
        res = 0;
    }
    return res;
}

void CopysetCheck::PrintHelp(const std::string& command) {
    std::cout << "Example: " << std::endl << std::endl;
    if (command == kCheckCopysetCmd) {
        std::cout << "curve_ops_tool check-copyset -logicalPoolId=2 "
            << "-copysetId=101 [-mdsAddr=127.0.0.1:6666] [-margin=1000] "
            << "[-confPath=/etc/curve/tools.conf]" << std::endl << std::endl;  // NOLINT
    } else if (command == kCheckChunnkServerCmd) {
        std::cout << "curve_ops_tool check-chunkserver "
            << "-chunkserverId=1 [-mdsAddr=127.0.0.1:6666] [-margin=1000] "
            << "[-confPath=/etc/curve/tools.conf]" << std::endl;
        std::cout << "curve_ops_tool check-chunkserver "
            << "[-mdsAddr=127.0.0.1:6666] "
            << "[-chunkserverAddr=127.0.0.1:8200] [-margin=1000] "
            << "[-confPath=/etc/curve/tools.conf]" << std::endl << std::endl;  // NOLINT
    } else if (command == kCheckServerCmd) {
        std::cout << "curve_ops_tool check-server -serverId=1 "
            << "[-mdsAddr=127.0.0.1:6666] [-margin=1000] "
            << "[-confPath=/etc/curve/tools.conf]" << std::endl;  // NOLINT
        std::cout << "curve_ops_tool check-server [-mdsAddr=127.0.0.1:6666] "
            << "[-serverIp=127.0.0.1] [-margin=1000] "
            << "[-confPath=/etc/curve/tools.conf]" << std::endl;  // NOLINT
    } else if (command == kCopysetsStatusCmd) {
        std::cout << "curve_ops_tool copysets-status [-mdsAddr=127.0.0.1:6666] "
                  << "[-margin=1000] [-operatorMaxPeriod=30] [-checkOperator] "
                  << "[-confPath=/etc/curve/tools.conf]" << std::endl << std::endl;  // NOLINT
    } else if (command == kCheckOperatorCmd) {
        std::cout << "curve_ops_tool check-operator -opName=" << kTotalOpName
                  << "/" << kChangeOpName << "/" << kAddOpName << "/"
                  << kRemoveOpName << "/" << kTransferOpName << std::endl;
    } else if (command == kListMayBrokenVolumes) {
        std::cout << "curve_ops_tool list-majoroff-vol" << std::endl;
    } else {
        std::cout << "Command not supported!" << std::endl;
    }
    std::cout << std::endl;
    std::cout << "Standard of healthy is no copyset in the following state:" << std::endl;  // NOLINT
    std::cout << "1、copyset has no leader" << std::endl;
    std::cout << "2、number of replicas less than expected" << std::endl;
    std::cout << "3、some replicas not online" << std::endl;
    std::cout << "4、installing snapshot" << std::endl;
    std::cout << "5、gap of log index between peers exceed margin" << std::endl;
    std::cout << "6、for check-cluster, it will also check whether the mds is scheduling if -checkOperator specified"  // NOLINT
                "(if no operators in operatorMaxPeriod, it considered healthy)" << std::endl;  // NOLINT
    std::cout << "By default, if the number of replicas is less than 3, it is considered unhealthy, "   // NOLINT
                 "you can change it by specify -replicasNum" << std::endl;
    std::cout << "The order is sorted by priority, if the former is satisfied, the rest will not be checked" << std::endl;  // NOLINT
}


void CopysetCheck::PrintStatistic() {
    const auto& statistics = core_->GetCopysetStatistics();
    std::cout << "total copysets: " << statistics.totalNum
              << ", unhealthy copysets: " << statistics.unhealthyNum
              << ", unhealthy_ratio: "
              << statistics.unhealthyRatio * 100 << "%" << std::endl;
}

void CopysetCheck::PrintDetail() {
    auto copysets = core_->GetCopysetsRes();
    std::ostream_iterator<std::string> out(std::cout, ", ");
    std::cout << std::endl;
    std::cout << "unhealthy copysets statistic: {";
    int i = 0;
    for (const auto& item : copysets) {
        if (item.first == kTotal) {
            continue;
        }
        if (i != 0) {
            std::cout << ", ";
        }
        i++;
        std::cout << item.first << ": ";
        std::cout << item.second.size();
    }
    std::cout << "}" << std::endl;
    std::cout << "unhealthy copysets list: " << std::endl;
    for (const auto& item : copysets) {
        if (item.first == kTotal) {
            continue;
        }
        std::cout << item.first << ": ";
        PrintCopySet(item.second);
    }
    std::cout << std::endl;
    // 打印有问题的chunkserver
    PrintExcepChunkservers();
}

void CopysetCheck::PrintCopySet(const std::set<std::string>& set) {
    std::cout << "{";
    for (auto iter = set.begin(); iter != set.end(); ++iter) {
        if (iter != set.begin()) {
            std::cout << ",";
        }
        std::string gid = *iter;
        uint64_t groupId;
        if (!curve::common::StringToUll(gid, &groupId)) {
            std::cout << "parse group id fail: " << groupId << std::endl;
            continue;
        }
        PoolIdType lgId = GetPoolID(groupId);
        CopySetIdType csId = GetCopysetID(groupId);
        std::cout << "(grouId: " << gid << ", logicalPoolId: "
                  << std::to_string(lgId) << ", copysetId: "
                  << std::to_string(csId) << ")";
    }
    std::cout << "}" << std::endl;
}

void CopysetCheck::PrintExcepChunkservers() {
    auto serviceExceptionChunkServers =
                        core_->GetServiceExceptionChunkServer();
    if (!serviceExceptionChunkServers.empty()) {
        std::ostream_iterator<std::string> out(std::cout, ", ");
        std::cout << "service-exception chunkservers (total: "
                  << serviceExceptionChunkServers.size() << "): {";
        std::copy(serviceExceptionChunkServers.begin(),
                        serviceExceptionChunkServers.end(), out);
        std::cout << "}" << std::endl;
    }
    auto copysetLoadExceptionCS =
                    core_->GetCopysetLoadExceptionChunkServer();
    if (!copysetLoadExceptionCS.empty()) {
        std::ostream_iterator<std::string> out(std::cout, ", ");
        std::cout << "copyset-load-exception chunkservers (total: "
                  << copysetLoadExceptionCS.size() << "): {";
        std::copy(copysetLoadExceptionCS.begin(),
                        copysetLoadExceptionCS.end(), out);
        std::cout << "}" << std::endl;
    }
}

int CopysetCheck::PrintMayBrokenVolumes() {
    std::vector<std::string> fileNames;
    int res = core_->ListMayBrokenVolumes(&fileNames);
    if (res != 0) {
        std::cout << "ListMayBrokenVolumes fail" << std::endl;
        return -1;
    }
    for (const auto& fileName : fileNames) {
        std::cout << fileName << std::endl;
    }
    std::cout << "total count: " << fileNames.size() << std::endl;
    return 0;
}

}  // namespace tool
}  // namespace curve

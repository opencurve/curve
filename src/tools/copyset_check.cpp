/*
 * Project: curve
 * Created Date: 2019-10-30
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#include "src/tools/copyset_check.h"

DEFINE_bool(detail, false, "list the copyset detail or not");
DEFINE_uint64(margin, 1000, "The threshold of the gap between peers");
DEFINE_uint32(logicalPoolId, 0, "logical pool id of copyset");
DEFINE_uint32(copysetId, 0, "copyset id");
DEFINE_uint32(chunkserverId, 0, "chunkserver id");
DEFINE_string(chunkserverAddr, "", "if specified, chunkserverId is not required");  // NOLINT
DEFINE_uint32(serverId, 0, "server id");
DEFINE_string(serverIp, "", "server ip");
DEFINE_uint32(replicasNum, 3, "the number of replicas that required");
DEFINE_uint64(retryTimes, 3, "rpc retry times");
DEFINE_uint64(rpcTimeout, 3000, "millisecond for rpc timeout");

namespace curve {
namespace tool {

int CopysetCheck::Init(const std::string& mdsAddr) {
    std::vector<std::string> mdsAddrVec;
    curve::common::SplitString(mdsAddr, ",", &mdsAddrVec);
    if (mdsAddrVec.empty()) {
        std::cout << "Split mds address fail!" << std::endl;
        return -1;
    }
    for (const auto& mdsAddr : mdsAddrVec) {
        if (channelToMds_->Init(mdsAddr.c_str(), nullptr) != 0) {
            std::cout << "Init channel to " << mdsAddr << "fail!" << std::endl;
            continue;
        }
        curve::mds::topology::TopologyService_Stub topo_stub(channelToMds_);
        curve::mds::topology::ListPhysicalPoolRequest request;
        curve::mds::topology::ListPhysicalPoolResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeout);
        topo_stub.ListPhysicalPool(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            continue;
        }
        mdsAddr_ = mdsAddr;
        return 0;
    }
    std::cout << "Init channel to all mds fail!" << std::endl;
    return -1;
}

CopysetCheck::CopysetCheck() {
    channelToMds_ = new (std::nothrow) brpc::Channel();
}

CopysetCheck::~CopysetCheck() {
    delete channelToMds_;
    channelToMds_ = nullptr;
}

int CopysetCheck::RunCommand(std::string command) {
    int res = 0;
    if (command == "check-copyset") {
        // 检查某个copyset的状态
        if (FLAGS_logicalPoolId == 0 || FLAGS_copysetId == 0) {
            std::cout << "logicalPoolId AND copysetId should be specified!"
                      << std::endl;
            return -1;
        }
        res = CheckOneCopyset(FLAGS_logicalPoolId, FLAGS_copysetId);
        std::cout << "Copyset ";
    } else if (command == "check-chunkserver") {
        // 检查某个chunkserver上的所有copyset
        if (FLAGS_chunkserverAddr.empty() && FLAGS_chunkserverId == 0) {
            std::cout << "chunkserverId OR chunkserverAddr should be secified!"
                      << std::endl;
            return -1;
        }
        if (!FLAGS_chunkserverAddr.empty() && FLAGS_chunkserverId != 0) {
            std::cout << "Only one of chunkserverId OR "
                         "chunkserverAddr should be secified!" << std::endl;
            return -1;
        }
        if (FLAGS_chunkserverId != 0) {
            res = CheckCopysetsOnChunkserver(FLAGS_chunkserverId);
        } else {
            res = CheckCopysetsOnChunkserver(FLAGS_chunkserverAddr);
        }
        PrintStatistic();
        std::cout << "Chunkserver ";
    } else if (command == "check-server") {
        if (FLAGS_serverIp.empty() && FLAGS_serverId == 0) {
            std::cout << "serverId OR serverIp should be secified!"
                      << std::endl;
            return -1;
        }
        if (!FLAGS_serverIp.empty() && FLAGS_serverId != 0) {
            std::cout << "Only one of serverId OR serverIp should be secified!"
                      << std::endl;
            return -1;
        }
        if (FLAGS_serverId != 0) {
            res = CheckCopysetsOnServer(FLAGS_serverId);
        } else {
            res = CheckCopysetsOnServer(FLAGS_serverIp);
        }
        PrintStatistic();
        std::cout << "Server ";
    } else if (command == "check-cluster") {
        res = CheckCopysetsInCluster();
        PrintStatistic();
        std::cout << "Cluster ";
    } else {
        PrintHelp(command);
        return -1;
    }
    if (res == 0) {
        std::cout << "is healthy!" << std::endl;
    } else {
        std::cout << "is not healthy!" << std::endl;
    }
    if (FLAGS_detail) {
        PrintDetail(command);
    }
    return res;
}

void CopysetCheck::PrintHelp(std::string command) {
    std::cout << "Example: " << std::endl << std::endl;
    if (command == "check-copyset") {
         std::cout << "curve_ops_tool check-copyset -mdsAddr=127.0.0.1:6666 "
         "-logicalPoolId=2 -copysetId=101 [-margin=1000]" << std::endl << std::endl;  // NOLINT
    } else if (command == "check-chunkserver") {
        std::cout << "curve_ops_tool check-chunkserver -mdsAddr=127.0.0.1:6666 "
        "-chunkserverId=1 [-margin=1000]" << std::endl;
        std::cout << "curve_ops_tool check-chunkserver -mdsAddr=127.0.0.1:6666 "
        "-chunkserverAddr=127.0.0.1:8200 [-margin=1000]" << std::endl << std::endl;  // NOLINT
    } else if (command == "check-server") {
        std::cout << "curve_ops_tool check-server -mdsAddr=127.0.0.1:6666 -serverId=1 [-margin=1000]" << std::endl;  // NOLINT
        std::cout << "curve_ops_tool check-server -mdsAddr=127.0.0.1:6666 -serverIp=127.0.0.1 [-margin=1000]" << std::endl;  // NOLINT
    } else if (command == "check-cluster") {
        std::cout << "curve_ops_tool check-cluster -mdsAddr=127.0.0.1:6666 [-margin=1000] [-copysetRange=10] [-leaderRange=10]" << std::endl << std::endl;  // NOLINT
    } else {
        std::cout << "Command not supported! Supported commands: " << std::endl;
        std::cout << "check-copyset" << std::endl;
        std::cout << "check-chunkserver" << std::endl;
        std::cout << "check-server" << std::endl;
        std::cout << "check-cluster" << std::endl;
    }
    std::cout << std::endl;
    std::cout << "Standard of healthy is no copyset in the following state:" << std::endl;  // NOLINT
    std::cout << "1、copyset has no leader" << std::endl;
    std::cout << "2、number of replicas less than expected" << std::endl;
    std::cout << "3、some replicas not online" << std::endl;
    std::cout << "4、installing snapshot" << std::endl;
    std::cout << "5、gap of log index between peers exceed margin" << std::endl;
    std::cout << "6、for check-cluster, the range of copyset number and leader number on chunkserver also should be considered" << std::endl;  // NOLINT
    std::cout << "By default, if the number of replicas is less than 3, it is considered unhealthy, "   // NOLINT
                 "you can change it by specify -replicasNum" << std::endl;
    std::cout << "The order is sorted by priority, if the former is satisfied, the rest will not be checked" << std::endl;  // NOLINT
}

int CopysetCheck::CheckOneCopyset(const PoolIdType& logicalPoolId,
                                  const CopySetIdType& copysetId) {
    curve::mds::topology::GetChunkServerListInCopySetsRequest mdsRequest;
    curve::mds::topology::GetChunkServerListInCopySetsResponse mdsResponse;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeout);
    mdsRequest.set_logicalpoolid(logicalPoolId);
    mdsRequest.add_copysetid(copysetId);
    curve::mds::topology::TopologyService_Stub topo_stub(channelToMds_);
    topo_stub.GetChunkServerListInCopySets(&cntl,
                            &mdsRequest, &mdsResponse, nullptr);
    if (cntl.Failed()) {
        std::cout << "GetChunkServerListInCopySets fail, errCode = "
                    << mdsResponse.statuscode()
                    << ", error content:"
                    << cntl.ErrorText() << std::endl;
        return -1;
    }
    if (mdsResponse.has_statuscode()) {
        if (mdsResponse.statuscode() != kTopoErrCodeSuccess) {
            std::cout << "GetChunkServerListInCopySets fail, errCode: "
                      << mdsResponse.statuscode() << std::endl;
            return -1;
        }
    }

    curve::mds::topology::CopySetServerInfo info = mdsResponse.csinfo(0);
    // 保存三个副本的地址，这样的话三个副本都不在线的情况也能知道有哪些peers
    std::vector<std::string> peers;
    for (int i = 0; i < info.cslocs_size(); ++i) {
        curve::mds::topology::ChunkServerLocation csl = info.cslocs(i);
        std::string hostIp = csl.hostip();
        uint64_t port = csl.port();
        std::string csAddr = hostIp + ":" + std::to_string(port);
        std::string groupId = ToGroupId(logicalPoolId, copysetId);
        ChunkserverHealthStatus res =
                            CheckCopysetsOnChunkserver(csAddr, {groupId});
        peers.push_back(csAddr);
        // 根据chunkserver检查的结果，健康返回0，不健康返回-1，不在线则继续查询
        if (res == ChunkserverHealthStatus::kHealthy) {
            return 0;
        } else if (res == ChunkserverHealthStatus::kNotHealthy) {
            return -1;
        }
    }
    // 三个副本都不在线
    // TODO(charisu) : 查询层和展示层分开
    std::cout << "All chunkservers not online!" << std::endl;
    if (FLAGS_detail) {
        std::cout << "Chunkservers in copyset: {";
        std::ostream_iterator<std::string> out(std::cout, ", ");
        std::copy(peers.begin(),
                        peers.end(), out);
        std::cout << "}" << std::endl;
    }
    return -1;
}

int CopysetCheck::CheckCopysetsOnChunkserver(
                        const ChunkServerIdType& chunkserverId) {
    return CheckCopysetsOnChunkserver(chunkserverId, "");
}

int CopysetCheck::CheckCopysetsOnChunkserver(
                        const std::string& chunkserverAddr) {
    return CheckCopysetsOnChunkserver(0, chunkserverAddr);
}

int CopysetCheck::CheckCopysetsOnChunkserver(
                        const ChunkServerIdType& chunkserverId,
                        const std::string& chunkserverAddr) {
    // 向mds发送RPC
    curve::mds::topology::GetChunkServerInfoRequest mdsRequest;
    curve::mds::topology::GetChunkServerInfoResponse mdsResponse;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeout);
    if (chunkserverId > 0) {
        mdsRequest.set_chunkserverid(chunkserverId);
    }
    if (!chunkserverAddr.empty()) {
        if (!curve::common::NetCommon::CheckAddressValid(chunkserverAddr)) {
            std::cout << "chunkserverAddr invalid!" << std::endl;
            return -1;
        }
        std::vector<std::string> strs;
        curve::common::SplitString(chunkserverAddr, ":", &strs);
        std::string ip = strs[0];
        uint64_t port;
        curve::common::StringToUll(strs[1], &port);
        mdsRequest.set_hostip(ip);
        mdsRequest.set_port(port);
    }
    curve::mds::topology::TopologyService_Stub topo_stub(channelToMds_);
    topo_stub.GetChunkServer(&cntl, &mdsRequest, &mdsResponse, nullptr);
    if (cntl.Failed()) {
        std::cout << "GetChunkServer fail, errCode = "
                    << mdsResponse.statuscode()
                    << ", error content:"
                    << cntl.ErrorText() << std::endl;
        return -1;
    }
    if (mdsResponse.has_statuscode()) {
        if (mdsResponse.statuscode() != kTopoErrCodeSuccess) {
            std::cout << "GetChunkServer fail, errCode: "
                      << mdsResponse.statuscode() << std::endl;
            return -1;
        }
    }
    curve::mds::topology::ChunkServerInfo csinfo =
                            mdsResponse.chunkserverinfo();
    // 如果chunkserver retired的话不发送请求
    if (csinfo.status() == ChunkServerStatus::RETIRED) {
        std::cout << "Chunkserver is retired!" << std::endl;
        return 0;
    }
    // 向chunkserver发送RPC请求获取raft state
    std::string hostIp = csinfo.hostip();
    uint64_t port = csinfo.port();
    std::string csAddr = hostIp + ":" + std::to_string(port);
    ChunkserverHealthStatus res = CheckCopysetsOnChunkserver(csAddr, {});
    if (res == ChunkserverHealthStatus::kHealthy) {
        return 0;
    } else {
        return -1;
    }
}

ChunkserverHealthStatus CopysetCheck::CheckCopysetsOnChunkserver(
                    const std::string& chunkserverAddr,
                    const std::set<std::string>& groupIds,
                    bool queryLeader) {
    bool isHealthy = true;
    butil::IOBuf iobuf;
    if (QueryChunkserver(chunkserverAddr, &iobuf) != 0) {
        // 如果查询chunkserver失败，认为不在线，把它上面所有的
        // copyset都添加到peerNotOnlineCopysets_里面
        UpdatePeerNotOnlineCopysets(chunkserverAddr);
        serviceExceptionChunkservers_.emplace(chunkserverAddr);
        return ChunkserverHealthStatus::kNotOnline;
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
        return ChunkserverHealthStatus::kNotHealthy;
    }
    // 存储需要发送消息的chunkserver的地址和对应的groupId
    // key是chunkserver地址，value是groupId的列表
    std::map<std::string, std::set<std::string>> csAddrMap;
    for (auto& copysetInfo : copysetInfos) {
        if (copysetInfo["state"] == "LEADER") {
            CheckResult res = CheckHealthOnLeader(&copysetInfo);
            switch (res) {
                case CheckResult::kPeersNoSufficient:
                    peerLessCopysets_.emplace(copysetInfo["groupId"]);
                    isHealthy = false;
                    break;
                case CheckResult::kLogIndexGapTooBig:
                    indexGapBigCopysets_.emplace(copysetInfo["groupId"]);
                    isHealthy = false;
                    break;
                case CheckResult::kInstallingSnapshot:
                    installSnapshotCopysets_.emplace(copysetInfo["groupId"]);
                    isHealthy = false;
                    break;
                case CheckResult::kPeerNotOnline:
                    peerNotOnlineCopysets_.emplace(copysetInfo["groupId"]);
                    isHealthy = false;
                    break;
                case CheckResult::kParseError:
                    std::cout << "Parse the result fail!" << std::endl;
                    isHealthy = false;
                    break;
                default:
                    break;
            }
        } else {
            // 如果没有leader，返回不健康
            if (copysetInfo["leader"] == "0.0.0.0:0:0") {
                noLeaderCopysets_.emplace(copysetInfo["groupId"]);
                isHealthy = false;
                continue;
            }
            if (queryLeader) {
                // 向leader发送rpc请求
                auto pos = copysetInfo["leader"].rfind(":");
                auto csAddr = copysetInfo["leader"].substr(0, pos);
                csAddrMap[csAddr].emplace(copysetInfo["groupId"]);
            }
        }
    }
    // 遍历chunkserver发送请求
    for (const auto& item : csAddrMap) {
        if (CheckCopysetsOnChunkserver(item.first, item.second)
                                != ChunkserverHealthStatus::kHealthy) {
            isHealthy = false;
        }
    }
    if (isHealthy) {
        return ChunkserverHealthStatus::kHealthy;
    } else {
        return ChunkserverHealthStatus::kNotHealthy;
    }
}

int CopysetCheck::CheckCopysetsOnServer(const ServerIdType& serverId) {
    return CheckCopysetsOnServer(serverId, "");
}

int CopysetCheck::CheckCopysetsOnServer(const std::string serverIp) {
    return CheckCopysetsOnServer(0, serverIp);
}

int CopysetCheck::CheckCopysetsOnServer(const ServerIdType& serverId,
                                        const std::string serverIp,
                                        bool queryLeader) {
    bool isHealthy = true;
    // 向mds发送RPC
    curve::mds::topology::ListChunkServerRequest mdsRequest;
    curve::mds::topology::ListChunkServerResponse mdsResponse;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeout);
    if (serverId > 0) {
        mdsRequest.set_serverid(serverId);
    }
    if (serverIp != "") {
        mdsRequest.set_ip(serverIp);
    }
    curve::mds::topology::TopologyService_Stub topo_stub(channelToMds_);
    topo_stub.ListChunkServer(&cntl, &mdsRequest, &mdsResponse, nullptr);
    if (cntl.Failed()) {
        std::cout << "ListChunkServer fail, errCode = "
                  << mdsResponse.statuscode()
                  << ", error content:"
                  << cntl.ErrorText() << std::endl;
        return -1;
    }
    if (mdsResponse.has_statuscode()) {
        if (mdsResponse.statuscode() != kTopoErrCodeSuccess) {
            std::cout << "ListChunkServer fail, errCode: "
                      << mdsResponse.statuscode() << std::endl;
            return -1;
        }
    }
    for (int i = 0; i < mdsResponse.chunkserverinfos_size(); ++i) {
        const auto& info = mdsResponse.chunkserverinfos(i);
        std::string ip = info.hostip();
        uint64_t port = info.port();
        std::string csAddr = ip + ":" + std::to_string(port);
        // 跳过retired状态的chunkserver
        if (info.status() == ChunkServerStatus::RETIRED) {
            continue;
        }
        ChunkserverHealthStatus res = CheckCopysetsOnChunkserver(csAddr,
                                                    {}, queryLeader);
        if (res != ChunkserverHealthStatus::kHealthy) {
            isHealthy = false;
            if (res == ChunkserverHealthStatus::kNotHealthy) {
                unhealthyChunkservers_.emplace(csAddr);
            }
        }
    }
    if (isHealthy) {
        return 0;
    }  else {
        return -1;
    }
}

int CopysetCheck::CheckCopysetsInCluster() {
    bool isHealthy = true;
    curve::mds::topology::TopologyService_Stub topo_stub(channelToMds_);
    // 先获取物理池列表
    curve::mds::topology::ListPhysicalPoolRequest request1;
    curve::mds::topology::ListPhysicalPoolResponse response1;
    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_rpcTimeout);
    topo_stub.ListPhysicalPool(&cntl, &request1, &response1, nullptr);
    if (cntl.Failed()) {
        std::cout << "ListPhysicalPool fail, errCode = "
                    << response1.statuscode()
                    << ", error content:"
                    << cntl.ErrorText() << std::endl;
        return -1;
    }
    if (response1.has_statuscode()) {
        if (response1.statuscode() != kTopoErrCodeSuccess) {
            std::cout << "ListPhysicalPool fail, errCode: "
                      << response1.statuscode() << std::endl;
            return -1;
        }
    }
    for (int i = 0; i < response1.physicalpoolinfos_size(); ++i) {
        const auto& info = response1.physicalpoolinfos(i);
        const auto& poolId = info.physicalpoolid();
        // 再获取每个物理池下的zone列表
        curve::mds::topology::ListPoolZoneRequest request2;
        curve::mds::topology::ListPoolZoneResponse response2;
        cntl.Reset();
        cntl.set_timeout_ms(FLAGS_rpcTimeout);
        request2.set_physicalpoolid(poolId);
        topo_stub.ListPoolZone(&cntl, &request2, &response2, nullptr);
        if (cntl.Failed()) {
            std::cout << "ListPoolZone fail, errCode = "
                      << response2.statuscode()
                      << ", error content:"
                      << cntl.ErrorText() << std::endl;
            return -1;
        }
        if (response2.has_statuscode()) {
            if (response2.statuscode() != kTopoErrCodeSuccess) {
                std::cout << "ListPoolZone fail, errCode: "
                      << response2.statuscode() << std::endl;
                return -1;
            }
        }
        for (int i = 0; i < response2.zones_size(); ++i) {
            const auto& zoneInfo = response2.zones(i);
            const auto& zoneId = zoneInfo.zoneid();
            // 获取每个zone下的server列表，然后检查健康状态
            curve::mds::topology::ListZoneServerRequest request3;
            curve::mds::topology::ListZoneServerResponse response3;
            cntl.Reset();
            cntl.set_timeout_ms(FLAGS_rpcTimeout);
            request3.set_zoneid(zoneId);
            topo_stub.ListZoneServer(&cntl, &request3, &response3, nullptr);
            if (cntl.Failed()) {
                std::cout << "ListZoneServer fail, errCode = "
                        << response3.statuscode()
                        << ", error content:"
                        << cntl.ErrorText() << std::endl;
                return -1;
            }
            if (response3.has_statuscode()) {
                if (response3.statuscode() != kTopoErrCodeSuccess) {
                    std::cout << "ListZoneServer fail, errCode: "
                          << response3.statuscode() << std::endl;
                    return -1;
                }
            }
            for (int i = 0; i < response3.serverinfo_size(); ++i) {
                const auto& serverInfo = response3.serverinfo(i);
                const auto& serverId = serverInfo.serverid();
                if (CheckCopysetsOnServer(serverId, "", false) != 0) {
                    isHealthy = false;
                }
            }
        }
    }
    uint64_t opNum = 0;
    if (!GetOperatorNum(&opNum)) {
        std::cout << "Get oparator num from mds fail!" << std::endl;
        return -1;
    }
    if (isHealthy) {
        if (opNum != 0) {
            std::cout << "Exists operators on mds, scheduling!" << std::endl;
            return -1;
        }
        return 0;
    }  else {
        return -1;
    }
}

bool CopysetCheck::GetOperatorNum(uint64_t* opNum) {
    brpc::Channel httpChannel;
    brpc::ChannelOptions options;
    brpc::Controller cntl;
    options.protocol = brpc::PROTOCOL_HTTP;
    if (httpChannel.Init(mdsAddr_.c_str(), &options) != 0) {
        std::cout << "Fail to initialize http channel";
        return false;
    }
    std::string metricName = "mds_scheduler_metric_operator_num";
    cntl.Reset();
    // 查询copysetnum_range
    cntl.http_request().uri() = mdsAddr_ + "/vars/" + metricName;
    httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        std::cout << "Get metric from mds fail!"
                  << std::endl;
        return false;
    }
    std::vector<std::string> strs;
    curve::common::SplitString(cntl.response_attachment().to_string(),
                                                            ":", &strs);
    if (!curve::common::StringToUll(strs[1], opNum)) {
        std::cout << "parse operator num fail!" << std::endl;
        return false;
    }
    return true;
}

void CopysetCheck::PrintStatistic() {
    // 打印统计信息
    uint64_t unhealthyNum = peerLessCopysets_.size()
                            + installSnapshotCopysets_.size()
                            + noLeaderCopysets_.size()
                            + indexGapBigCopysets_.size()
                            + peerNotOnlineCopysets_.size();
    double unhealthyRatio = 0;
    if (totalCopysets_.size() != 0) {
        unhealthyRatio =
                static_cast<double>(unhealthyNum) / totalCopysets_.size();
    }
    std::cout << "total copysets: " << totalCopysets_.size()
              << ", unhealthy copysets: " << unhealthyNum
              << ", unhealthy_ratio: "
              << unhealthyRatio * 100 << "%" << std::endl;
}

std::string CopysetCheck::ToGroupId(const PoolIdType& logicPoolId,
                                           const CopySetIdType& copysetId) {
    uint64_t groupId = (static_cast<uint64_t>(logicPoolId) << 32) | copysetId;
    return std::to_string(groupId);
}

void CopysetCheck::ParseResponseAttachment(const std::set<std::string>& gIds,
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
                    // 查询单个copyset的时候打印
                    if (FLAGS_detail && FLAGS_copysetId > 0) {
                        std::cout << "groupId: " << gid << std::endl;
                        std::cout << copyset << std::endl << std::endl;
                    }
                    temp.clear();
                    map["groupId"] = gid;
                    if (gIds.empty()) {
                        // 查询chunkserver上所有copyset的时候保存，避免重复
                        totalCopysets_.emplace(gid);
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
                map[key] = "";
            } else {
                map[key] = line.substr(pos + 2);
            }
            temp.clear();
        }
        if (!map.empty()) {
            maps->push_back(map);
        }
        copyset.clear();
    }
}

int CopysetCheck::QueryChunkserver(const std::string& chunkserverAddr,
                                   butil::IOBuf* iobuf) {
    brpc::Channel channel;
    if (channel.Init(chunkserverAddr.c_str(), nullptr) != 0) {
        std::cout << "Init channel to chunkserver: " << chunkserverAddr
                  << " failed!" << std::endl;
        return -1;
    }
    braft::IndexRequest idxRequest;
    braft::IndexResponse idxResponse;
    brpc::Controller cntl;
    int retryTimes = 0;
    braft::raft_stat_Stub raft_stub(&channel);
    while (retryTimes < FLAGS_retryTimes) {
        cntl.Reset();
        raft_stub.default_method(&cntl, &idxRequest, &idxResponse, nullptr);
        if (!cntl.Failed()) {
            iobuf->append(cntl.response_attachment());
            chunkserverStatus_[chunkserverAddr] = true;
            return 0;
        }
        retryTimes++;
    }
    // 为了方便起见，只打印最后一次失败的error text
    std::cout << "Send RPC to chunkserver fail, error content: "
              << cntl.ErrorText() << std::endl;
    chunkserverStatus_[chunkserverAddr] = false;
    return -1;
}

bool CopysetCheck::CheckChunkserverOnline(const std::string& chunkserverAddr) {
    // 如果已经查询过，就直接返回结果，每次查询的总时间很短，
    // 假设这段时间内chunkserver不会恢复
    if (chunkserverStatus_.find(chunkserverAddr) != chunkserverStatus_.end()) {
        return chunkserverStatus_[chunkserverAddr];
    }
    brpc::Channel channel;
    if (channel.Init(chunkserverAddr.c_str(), nullptr) != 0) {
        return false;
    }
    curve::chunkserver::GetChunkInfoRequest request;
    curve::chunkserver::GetChunkInfoResponse response;
    request.set_logicpoolid(1);
    request.set_copysetid(1);
    request.set_chunkid(1);
    brpc::Controller cntl;
    int retryTimes = 0;
    curve::chunkserver::ChunkService_Stub stub(&channel);
    while (retryTimes < FLAGS_retryTimes) {
        cntl.Reset();
        stub.GetChunkInfo(&cntl, &request, &response, nullptr);
        if (!cntl.Failed()) {
            chunkserverStatus_[chunkserverAddr] = true;
            return true;
        }
        retryTimes++;
    }
    // 为了方便起见，只打印最后一次失败的error text
    std::cout << "Send RPC to chunkserver fail, error content: "
              << cntl.ErrorText() << std::endl;
    chunkserverStatus_[chunkserverAddr] = false;
    return false;
}

CheckResult CopysetCheck::CheckHealthOnLeader(
                std::map<std::string, std::string>* map) {
    // 先判断peers是否小于3
    std::vector<std::string> peers;
    curve::common::SplitString((*map)["peers"], " ", &peers);
    if (peers.size() < FLAGS_replicasNum) {
        return CheckResult::kPeersNoSufficient;
    }
    // 检查三个peers是否都在线
    for (const auto& peer : peers) {
        auto pos = peer.rfind(":");
        if (pos == peer.npos) {
            std::cout << "parse peer fail!" << std::endl;
            return CheckResult::kParseError;
        }
        std::string csAddr = peer.substr(0, pos);
        if (!CheckChunkserverOnline(csAddr)) {
            serviceExceptionChunkservers_.emplace(csAddr);
            return CheckResult::kPeerNotOnline;
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
    if (!curve::common::StringToUll(str.substr(pos1 + 1, pos2 - pos1 - 1),
                                                        &lastLogId)) {
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
                if (!curve::common::StringToUll(
                        info.substr(pos + 1), &nextIndex)) {
                    std::cout << "parse next index fail!" << std::endl;
                    return CheckResult::kParseError;
                }
            }
            if (info.substr(0, pos) == "flying_append_entries_size") {
                if (!curve::common::StringToUll(info.substr(pos + 1),
                                                            &flying)) {
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

void CopysetCheck::PrintDetail(const std::string& command) {
    // 如果是检查单个copyset则不打印以下信息
    std::ostream_iterator<std::string> out(std::cout, ", ");
    if (command == "check-copyset") {
        std::cout << "service-exception chunkservers (total: "
                  << serviceExceptionChunkservers_.size() << "): {";
        std::copy(serviceExceptionChunkservers_.begin(),
                            serviceExceptionChunkservers_.end(), out);
        std::cout << "}" << std::endl;
        return;
    }
    std::cout << "unhealthy copysets statistic: {";
    std::cout << "peers not sufficient: " << peerLessCopysets_.size()
              << ", index gap too big: " << indexGapBigCopysets_.size()
              << ", installing snapshot: " << installSnapshotCopysets_.size()
              << ", no leader: " << noLeaderCopysets_.size()
              << ", peer not online: " << peerNotOnlineCopysets_.size() << "}"
              << std::endl << std::endl;
    std::cout << "unhealthy copysets list: " << std::endl;
    std::cout << "peers not sufficient: ";
    PrintSet(peerLessCopysets_);
    std::cout << std::endl;
    std::cout << "index gap too big: ";
    PrintSet(indexGapBigCopysets_);
    std::cout << std::endl;
    std::cout << "installing snapshot: ";
    PrintSet(installSnapshotCopysets_);
    std::cout << std::endl;
    std::cout << "no leader: ";
    PrintSet(noLeaderCopysets_);
    std::cout << std::endl;
    std::cout << "peer not online: ";
    PrintSet(peerNotOnlineCopysets_);
    std::cout << std::endl;
    // 打印offline的chunkserver，这里不能严格说它offline，
    // 有可能是chunkserver起来了但无法提供服务,所以叫Service-exception
    std::cout << "service-exception chunkserver list (total: "
              << serviceExceptionChunkservers_.size() <<"): {";
    std::copy(serviceExceptionChunkservers_.begin(),
                        serviceExceptionChunkservers_.end(), out);
    std::cout << "}" << std::endl << std::endl;
    if (command == "check-server") {
        std::cout << "unhealthy chunkserver list (total: "
                  << unhealthyChunkservers_.size() << "): {";
    }
}

void CopysetCheck::PrintSet(const std::set<std::string>& set) {
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
        PoolIdType lgId = groupId >> 32;
        CopySetIdType csId = groupId & (((uint64_t)1 << 32) - 1);
        std::cout << "(grouId: " << gid << ", logicalPoolId: "
                  << std::to_string(lgId) << ", copysetId: "
                  << std::to_string(csId) << ")";
    }
    std::cout << "}" << std::endl;
}

void CopysetCheck::UpdatePeerNotOnlineCopysets(const std::string& csAddr) {
    curve::mds::topology::GetCopySetsInChunkServerRequest mdsRequest;
    curve::mds::topology::GetCopySetsInChunkServerResponse mdsResponse;
    brpc::Controller cntl;
    if (!curve::common::NetCommon::CheckAddressValid(csAddr)) {
        std::cout << "chunkserverAddr invalid!" << std::endl;
        return;
    }
    std::vector<std::string> strs;
    curve::common::SplitString(csAddr, ":", &strs);
    std::string ip = strs[0];
    uint64_t port;
    curve::common::StringToUll(strs[1], &port);
    mdsRequest.set_hostip(ip);
    mdsRequest.set_port(port);
    curve::mds::topology::TopologyService_Stub topo_stub(channelToMds_);
    cntl.set_timeout_ms(FLAGS_rpcTimeout);
    topo_stub.GetCopySetsInChunkServer(&cntl, &mdsRequest,
                                            &mdsResponse, nullptr);
    if (cntl.Failed()) {
        std::cout << "GetCopySetsInChunkServer fail, errCode = "
                  << mdsResponse.statuscode()
                  << ", error content:"
                  << cntl.ErrorText() << std::endl;
        return;
    }
    if (mdsResponse.has_statuscode()) {
        if (mdsResponse.statuscode() != kTopoErrCodeSuccess) {
            std::cout << "GetCopySetsInChunkServer fail, errCode: "
                      << mdsResponse.statuscode() << std::endl;
            return;
        }
    }
    for (int i = 0; i < mdsResponse.copysetinfos_size(); ++i) {
        const auto& csInfo = mdsResponse.copysetinfos(i);
        PoolIdType logicalPoolId = csInfo.logicalpoolid();
        CopySetIdType copysetId = csInfo.copysetid();
        std::string groupId = ToGroupId(logicalPoolId,
                                        copysetId);
        peerNotOnlineCopysets_.emplace(groupId);
        totalCopysets_.emplace(groupId);
    }
}

}  // namespace tool
}  // namespace curve

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

namespace curve {
namespace tool {

int CopysetCheck::Init(const std::string& mdsAddr) {
    curve::common::SplitString(mdsAddr, ",", &mdsAddrVec_);
    if (mdsAddrVec_.empty()) {
        std::cout << "Split mds address fail!" << std::endl;
        return -1;
    }
    for (const auto& mdsAddr : mdsAddrVec_) {
        if (channelToMds_->Init(mdsAddr.c_str(), nullptr) != 0) {
            std::cout << "Init channel to " << mdsAddr << "fail!" << std::endl;
            continue;
        }
        curve::mds::topology::TopologyService_Stub topo_stub(channelToMds_);
        curve::mds::topology::ListPhysicalPoolRequest request;
        curve::mds::topology::ListPhysicalPoolResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(3000);
        topo_stub.ListPhysicalPool(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            continue;
        }
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
        std::cout << "Server ";
    } else if (command == "check-cluster") {
        res = CheckCopysetsInCluster();
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
        std::cout << "curve_ops_tool check-cluster -mdsAddr=127.0.0.1:6666 [-margin=1000]" << std::endl << std::endl;  // NOLINT
    } else {
        std::cout << "Command not supported! Supported commands: " << std::endl;
        std::cout << "check-copyset" << std::endl;
        std::cout << "check-chunkserver" << std::endl;
        std::cout << "check-server" << std::endl;
        std::cout << "check-cluster" << std::endl;
    }
    std::cout << std::endl;
    std::cout << "Standard of healthy is no copyset in the following state:" << std::endl;  // NOLINT
    std::cout << "1、installing snapshot" << std::endl;
    std::cout << "2、gap of log index between peers exceed margin" << std::endl;
    std::cout << "3、copyset has no leader" << std::endl;
    std::cout << "4、number of replicas less than expected" << std::endl;
    std::cout << "By default, if the number of replicas is less than 3, it is considered unhealthy, "   // NOLINT
                 "you can change it by specify -replicasNum" << std::endl;
}

int CopysetCheck::CheckOneCopyset(const PoolIdType& logicalPoolId,
                                  const CopySetIdType& copysetId) {
    curve::mds::topology::GetChunkServerListInCopySetsRequest mdsRequest;
    curve::mds::topology::GetChunkServerListInCopySetsResponse mdsResponse;
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
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

    // 先向第一个chunkserver发送请求，如果是follower且leader不为空就向leader
    // 发请求。如果没有leader，认为不健康
    curve::mds::topology::CopySetServerInfo info = mdsResponse.csinfo(0);
    int chunkServerNum = info.cslocs_size();
    if (chunkServerNum < 3) {
        std::cout << "Number of chunkserver less than 3" << std::endl;
        return -1;
    }
    curve::mds::topology::ChunkServerLocation csl = info.cslocs(0);
    std::string hostIp = csl.hostip();
    uint64_t port = csl.port();
    std::string csAddr = hostIp + ":" + std::to_string(port);
    while (true) {
        butil::IOBuf iobuf;
        // copyset不会包含retired的chunkserver，如果chunkserver连接失败，
        // 认为不健康
        if (QueryChunkserver(csAddr, &iobuf) != 0) {
            std::cout << "Send RPC to chunkserver " << csAddr
                      << " fail!" << std::endl;
            return -1;
        }
        // 从response_attachment里面解析出copyset信息存到map里面
        std::vector<std::map<std::string, std::string>> copysetInfos;
        std::string groupId = ToGroupId(logicalPoolId, copysetId);
        std::set<std::string> groupIds = {groupId};
        ParseResponseAttachment(groupIds, &iobuf, &copysetInfos);
        // 从map根据状态做出判断
        // 1.如果是leader，调用CheckHealthOnLeader从map分析是否健康
        if (copysetInfos.empty()) {
            std::cout << "Copyset not found on chunkserver!" << std::endl;
            return -1;
        }
        auto map = copysetInfos[0];
        if (map["state"] == "LEADER") {
            CheckResult res = CheckHealthOnLeader(&map);
            switch (res) {
                case CheckResult::kPeersNoSufficient:
                    std::cout << "Numbser of peers not sufficient!" << std::endl;  //NOLINT
                    return -1;
                case CheckResult::kLogIndexGapTooBig:
                    std::cout << "Gap between log index exceed margin!"
                          << std::endl;
                    return -1;
                case CheckResult::kInstallingSnapshot:
                    std::cout << "Copyset is installing snapshot!"
                              << std::endl;
                    return -1;
                case CheckResult::kParseError:
                    std::cout << "Parse the result fail!" << std::endl;
                    return -1;
                case CheckResult::kHealthy:
                    return 0;
                default:
                    break;
            }
        } else  {
            // 2.如果没有leader，返回不健康
            if (map["leader"] == "0.0.0.0:0:0") {
                std::cout << "Copyset has no leader!" << std::endl;
                return -1;
            }
            // 3.向leader发送rpc请求
            auto pos = map["leader"].rfind(":");
            csAddr = map["leader"].substr(0, pos);
        }
    }
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
    cntl.set_timeout_ms(3000);
    if (chunkserverId > 0) {
        mdsRequest.set_chunkserverid(chunkserverId);
    }
    if (chunkserverAddr != "") {
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
    int res = CheckCopysetsOnChunkserver(csAddr, {});
    return res;
}

int CopysetCheck::CheckCopysetsOnChunkserver(
                    const std::string& chunkserverAddr,
                    const std::set<std::string>& groupIds,
                    bool queryLeader) {
    bool isHealthy = true;
    butil::IOBuf iobuf;
    if (QueryChunkserver(chunkserverAddr, &iobuf) != 0) {
        std::cout << "Send RPC to chunkserver " << chunkserverAddr
                  << " fail!" << std::endl;
        return -1;
    }
    // 存储每一个copyset的详细信息
    std::vector<std::map<std::string, std::string>> copysetInfos;
    ParseResponseAttachment(groupIds, &iobuf, &copysetInfos);
    // 对应的chunkserver上没有要找的leader的copyset，可能已经迁移出去了，
    // 但是follower这边还没更新，这种情况也chunkserver认为不健康
    if (copysetInfos.empty() ||
            (!groupIds.empty() && copysetInfos.size() != groupIds.size())) {
        std::cout << "Some copysets not found on chunkserver, may be tranfered"
                  << std::endl;
        return -1;
    }
    // 存储需要发送消息的chunkserver的地址和对应的groupId
    // key是chunkserver地址，value是groupId的列表
    std::map<std::string, std::set<std::string>> csAddrMap;
    for (auto& copysetInfo : copysetInfos) {
        if (copysetInfo["state"] == "LEADER") {
            CheckResult res = CheckHealthOnLeader(&copysetInfo);
            switch (res) {
                case CheckResult::kPeersNoSufficient:
                    peerLessCopysets_.push_back(copysetInfo["groupId"]);
                    isHealthy = false;
                    break;
                case CheckResult::kLogIndexGapTooBig:
                    indexGapBigCopysets_.push_back(copysetInfo["groupId"]);
                    isHealthy = false;
                    break;
                case CheckResult::kInstallingSnapshot:
                    installSnapshotCopysets_.push_back(copysetInfo["groupId"]);
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
                noLeaderCopysets_.push_back(copysetInfo["groupId"]);
                isHealthy = false;
                continue;
            }
            if (queryLeader) {
                // 向leader发送rpc请求
                auto pos = copysetInfo["leader"].rfind(":");
                auto csAddr = copysetInfo["leader"].substr(0, pos);
                csAddrMap[csAddr].insert(copysetInfo["groupId"]);
            }
        }
    }
    // 遍历chunkserver发送请求
    for (const auto& item : csAddrMap) {
        if (CheckCopysetsOnChunkserver(item.first, item.second) != 0) {
            isHealthy = false;
        }
    }
    if (isHealthy) {
        return 0;
    } else {
        return -1;
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
    cntl.set_timeout_ms(3000);
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
        if (CheckCopysetsOnChunkserver(csAddr, {}, queryLeader) != 0) {
            unhealthyChunkservers_.push_back(csAddr);
            isHealthy = false;
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
    cntl.set_timeout_ms(3000);
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
        cntl.set_timeout_ms(3000);
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
            cntl.set_timeout_ms(3000);
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
                    unhealthyServers_.push_back(serverInfo.hostname());
                    isHealthy = false;
                }
            }
        }
    }

    if (isHealthy) {
        return 0;
    }  else {
        return -1;
    }
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
            return 0;
        }
        retryTimes++;
    }
    // 为了方便起见，只打印最后一次失败的error text
    std::cout << "Send RPC to chunkserver fail, error content: "
              << cntl.ErrorText() << std::endl;
    return -1;
}

CheckResult CopysetCheck::CheckHealthOnLeader(
                std::map<std::string, std::string>* map) {
    // 先判断peers是否小于3
    std::vector<std::string> peers;
    curve::common::SplitString((*map)["peers"], " ", &peers);
    if (peers.size() < FLAGS_replicasNum) {
        return CheckResult::kPeersNoSufficient;
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
    if (command == "check-copyset") {
        return;
    }
    std::cout << "Unhealthy copysets statistic:" << std::endl;
    std::cout << "peers not sufficient: " << peerLessCopysets_.size()
              << " index gap too big: " << indexGapBigCopysets_.size()
              << " installing snapshot: " << installSnapshotCopysets_.size()
              << " no leader: " << noLeaderCopysets_.size() << std::endl;
    std::cout << "Unhealthy copysets list: " << std::endl;
    std::cout << "peers not sufficient: " << std::endl;
    PrintVec(peerLessCopysets_);
    std::cout << "index gap too big: " << std::endl;
    PrintVec(indexGapBigCopysets_);
    std::cout << "installing snapshot: " << std::endl;
    PrintVec(installSnapshotCopysets_);
    std::cout << "no leader: " << std::endl;
    PrintVec(noLeaderCopysets_);
    // 如果是检查单个chunkserver则不打印以下信息
    if (command == "check-chunkserver") {
        return;
    }
    std::cout << std::endl;
    std::cout << "Unhealthy chunkserver list: {";
    for (uint32_t i = 0; i < unhealthyChunkservers_.size(); ++i) {
        if (i != 0) {
            std::cout << ", ";
        }
        std::cout << unhealthyChunkservers_[i];
    }
    std::cout << "}" << std::endl;
    // 如果是检查单个server则不打印以下信息
    if (command == "check-server") {
        return;
    }
    std::cout << std::endl;
    std::cout << "Unhealthy server list: {";
    for (uint32_t i = 0; i < unhealthyServers_.size(); ++i) {
        if (i != 0) {
            std::cout << ",";
        }
        std::cout << unhealthyServers_[i];
    }
    std::cout << "}" << std::endl;
}

void CopysetCheck::PrintVec(const std::vector<std::string>& vec) {
    std::cout << "{";
    for (uint32_t i = 0; i < vec.size(); ++i) {
        if (i != 0) {
            std::cout << ",";
        }
        std::string gid = vec[i];
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
}  // namespace tool
}  // namespace curve

/*
 * Project: curve
 * Created Date: 2019-10-30
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#include "src/tools/copyset_check.h"

DEFINE_string(mdsAddr, "127.0.0.1:6666", "mds addr");
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

int CopysetCheck::Init() {
    curve::common::SplitString(FLAGS_mdsAddr, ",", &mdsAddrVec_);
    if (mdsAddrVec_.empty()) {
        std::cout << "Split mds address fail!" << std::endl;
        return -1;
    }
    channelToMds_ = new (std::nothrow) brpc::Channel();
    for (const auto& mdsAddr : mdsAddrVec_) {
        if (channelToMds_->Init(mdsAddr.c_str(), nullptr) != 0) {
            std::cout << "Init channel to " << mdsAddr << "fail!" << std::endl;
            continue;
        }
        curve::mds::topology::TopologyService_Stub topo_stub(channelToMds_);
        curve::mds::topology::ListPhysicalPoolRequest request;
        curve::mds::topology::ListPhysicalPoolResponse response;
        brpc::Controller cntl;
        topo_stub.ListPhysicalPool(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            continue;
        }
        return 0;
    }
    std::cout << "Init channel to all mds fail!" << std::endl;
    return -1;
}

CopysetCheck::~CopysetCheck() {
    delete channelToMds_;
    channelToMds_ = nullptr;
}

int CopysetCheck::RunCommand(std::string command) {
    if (command == "check-copyset") {
        // 检查某个copyset的状态
        if (FLAGS_logicalPoolId == 0 || FLAGS_copysetId == 0) {
            std::cout << "logicalPoolId AND copysetId should be specified!"
                      << std::endl;
            return -1;
        }
        return CheckOneCopyset(FLAGS_logicalPoolId, FLAGS_copysetId);
    } else if (command == "check-chunkserver") {
        // 检查某个chunkserver上的所有copyset
        if (FLAGS_chunkserverAddr.empty() && FLAGS_chunkserverId == 0) {
            std::cout << "chunkserverId OR chunkserverAddr should be secified!"
                      << std::endl;
            return -1;
        }
        if (!FLAGS_chunkserverAddr.empty()) {
            int res = CheckCopysetsOnChunkserver(FLAGS_chunkserverAddr);
            if (res == 0) {
                std::cout << "Chunkserver is healthy!" << std::endl;
            } else {
                std::cout << "Chunkserver is not healthy!" << std::endl;
            }
            return res;
        }
        return CheckCopysetsOnChunkserver(FLAGS_chunkserverId);
    } else if (command == "check-server") {
        if (FLAGS_serverIp.empty() && FLAGS_serverId == 0) {
            std::cout << "serverId OR serverIp should be secified!"
                      << std::endl;
            return -1;
        }
        int res = CheckCopysetsOnServer(FLAGS_serverId, FLAGS_serverIp);
        if (res == 0) {
            std::cout << "Server is healthy!" << std::endl;
        } else {
            std::cout << "Server is not healthy!" << std::endl;
        }
        return res;
    } else if (command == "check-cluster") {
        return CheckCopysetsInCluster();
    } else {
        PrintHelp(command);
        return -1;
    }
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
        if (QueryChunkserver(csAddr, &iobuf) != 0) {
            std::cout << "Send RPC to chunkserver fail!" << std::endl;
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
            std::cout << "Copyset not found!" << std::endl;
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
                    std::cout << "Copyset is healthy!" << std::endl;
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
    // 向mds发送RPC
    curve::mds::topology::GetChunkServerInfoRequest mdsRequest;
    curve::mds::topology::GetChunkServerInfoResponse mdsResponse;
    brpc::Controller cntl;
    mdsRequest.set_chunkserverid(chunkserverId);
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
    // 向chunkserver发送RPC请求获取raft state
    curve::mds::topology::ChunkServerInfo csinfo =
                            mdsResponse.chunkserverinfo();
    std::string hostIp = csinfo.hostip();
    uint64_t port = csinfo.port();
    std::string csAddr = hostIp + ":" + std::to_string(port);
    int res = CheckCopysetsOnChunkserver(csAddr);
    if (res == 0) {
        std::cout << "Chunkserver is healthy!" << std::endl;
    } else {
        std::cout << "Chunkserver is not healthy!" << std::endl;
    }
    return res;
}

int CopysetCheck::CheckCopysetsOnChunkserver(
                    const std::string& chunkserverAddr,
                    const std::set<std::string>& groupIds,
                    bool queryLeader) {
    butil::IOBuf iobuf;
    if (QueryChunkserver(chunkserverAddr, &iobuf) != 0) {
            std::cout << "Send RPC to chunkserver fail!" << std::endl;
            return -1;
    }
    // 存储每一个copyset的详细信息
    std::vector<std::map<std::string, std::string>> copysetInfos;
    ParseResponseAttachment(groupIds, &iobuf, &copysetInfos);
    if (copysetInfos.empty()) {
        std::cout << "No copyset found!" << std::endl;
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
                    break;
                case CheckResult::kLogIndexGapTooBig:
                    indexGapBigCopysets_.push_back(copysetInfo["groupId"]);
                    break;
                case CheckResult::kInstallingSnapshot:
                    installSnapshotCopysets_.push_back(copysetInfo["groupId"]);
                    break;
                case CheckResult::kParseError:
                    std::cout << "Parse the result fail!" << std::endl;
                    break;
                default:
                    break;
            }
        } else {
            // 如果没有leader，返回不健康
            if (copysetInfo["leader"] == "0.0.0.0:0:0") {
                noLeaderCopysets_.push_back(copysetInfo["groupId"]);
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
        // 这个结果不用判断，因为都会在有问题的copyset的vector里面更新
        CheckCopysetsOnChunkserver(item.first, item.second);
    }
    // 如果是从其他chunkserver发过来的请求，不打印detail，避免重复打印
    if (FLAGS_detail && groupIds.empty()) {
        PrintDetail();
    }
    if (peerLessCopysets_.empty() && indexGapBigCopysets_.empty() &&
            installSnapshotCopysets_.empty() && noLeaderCopysets_.empty()) {
        return 0;
    } else {
        return -1;
    }
}

int CopysetCheck::CheckCopysetsOnServer(const ServerIdType& serverId,
                                        const std::string serverIp,
                                        bool queryLeader) {
    // 向mds发送RPC
    curve::mds::topology::ListChunkServerRequest mdsRequest;
    curve::mds::topology::ListChunkServerResponse mdsResponse;
    brpc::Controller cntl;
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
    std::vector<std::string> unhealthyChunkservers;
    for (int i = 0; i < mdsResponse.chunkserverinfos_size(); ++i) {
        const auto& info = mdsResponse.chunkserverinfos(i);
        std::string ip = info.hostip();
        uint64_t port = info.port();
        std::string csAddr = ip + ":" + std::to_string(port);
        if (CheckCopysetsOnChunkserver(csAddr, {}, queryLeader) != 0) {
            unhealthyChunkservers.push_back(csAddr);
        }
    }
    if (FLAGS_detail) {
        std::cout << "Unhealthy chunkserver list: {";
        for (uint32_t i = 0; i < unhealthyChunkservers.size(); ++i) {
            if (i != 0) {
                std::cout << ", ";
            }
            std::cout << unhealthyChunkservers[i];
        }
        std::cout << "}" << std::endl;
    }
    if (!unhealthyChunkservers.empty()) {
        return -1;
    }
    return 0;
}

int CopysetCheck::CheckCopysetsInCluster() {
    curve::mds::topology::TopologyService_Stub topo_stub(channelToMds_);
    // 记录不健康的server的hostname
    std::vector<std::string> unhealthyServers;
    // 先获取物理池列表
    curve::mds::topology::ListPhysicalPoolRequest request1;
    curve::mds::topology::ListPhysicalPoolResponse response1;
    brpc::Controller cntl;
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
                    unhealthyServers.push_back(serverInfo.hostname());
                }
            }
        }
    }

    if (FLAGS_detail) {
        std::cout << "Unhealthy server list: {";
        for (uint32_t i = 0; i < unhealthyServers.size(); ++i) {
            if (i != 0) {
                std::cout << ",";
            }
            std::cout << unhealthyServers[i];
        }
        std::cout << "}" << std::endl;
    }
    if (!unhealthyServers.empty()) {
        std::cout << "Cluster is not healthy!" << std::endl;
        return -1;
    }  else {
        std::cout << "Cluster is healthy!" << std::endl;
        return 0;
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
                std::cout << "parse line fail!" << std::endl;
                temp.clear();
                continue;
            }
            std::string key = line.substr(0, pos);
            // 如果是replicator，把key简化一下
            if (key.find("replicator") != key.npos) {
                key = "replicator" + std::to_string(i);
                ++i;
            }
            map[key] = line.substr(pos + 2);
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
    while (retryTimes < FLAGS_retryTimes) {
        braft::raft_stat_Stub raft_stub(&channel);
        raft_stub.default_method(&cntl, &idxRequest, &idxResponse, nullptr);
        if (!cntl.Failed()) {
            iobuf->append(cntl.response_attachment());
            return 0;
        }
        std::cout << "GetCopysetStatus fail, error content:"
                  << cntl.ErrorText() << std::endl;
        retryTimes++;
    }
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
                if (info.find("installing snapshot") != info.npos) {
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

void CopysetCheck::PrintDetail() {
    std::cout << "Unhealthy copysets statistic:" << std::endl;
    std::cout << "peers less than 3: " << peerLessCopysets_.size()
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
                  << std::to_string(csId) << "), ";
    }
    std::cout << "}" << std::endl;
}
}  // namespace tool
}  // namespace curve

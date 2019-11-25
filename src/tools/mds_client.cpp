/*
 * Project: curve
 * Created Date: 2019-11-25
 * Author: charisu
 * Copyright (c) 2018 netease
 */
#include "src/tools/mds_client.h"
DECLARE_uint64(rpcTimeout);
DECLARE_uint64(rpcRetryTimes);

namespace curve {
namespace tool {

int MDSClient::Init(const std::string& mdsAddr) {
    if (isInited_) {
        return 0;
    }
    // 初始化channel
    curve::common::SplitString(mdsAddr, ",", &mdsAddrVec_);
    if (mdsAddrVec_.empty()) {
        std::cout << "Split mds address fail!" << std::endl;
        return -1;
    }
    for (uint64_t i = 0; i < mdsAddrVec_.size(); ++i) {
        if (channel_.Init(mdsAddrVec_[i].c_str(), nullptr) != 0) {
            std::cout << "Init channel to " << mdsAddr << "fail!" << std::endl;
            continue;
        }
        // 寻找哪个mds存活
        curve::mds::GetFileInfoRequest request;
        curve::mds::GetFileInfoResponse response;
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeout);
        request.set_filename("/");
        FillUserInfo(&request);
        curve::mds::CurveFSService_Stub stub(&channel_);
        stub.GetFileInfo(&cntl, &request, &response, nullptr);

        if (cntl.Failed()) {
            continue;
        }
        currentMdsIndex_ = i;
        isInited_ = true;
        return 0;
    }
    std::cout << "Init channel to all mds fail!" << std::endl;
    return -1;
}

int MDSClient::GetFileInfo(const std::string &fileName,
                           FileInfo* fileInfo) {
    if (!fileInfo) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    curve::mds::GetFileInfoRequest request;
    curve::mds::GetFileInfoResponse response;
    request.set_filename(fileName);
    FillUserInfo(&request);
    curve::mds::CurveFSService_Stub stub(&channel_);

    void (curve::mds::CurveFSService_Stub::*fp)(
                            google::protobuf::RpcController*,
                            const curve::mds::GetFileInfoRequest*,
                            curve::mds::GetFileInfoResponse*,
                            google::protobuf::Closure*);
    fp = &curve::mds::CurveFSService_Stub::GetFileInfo;
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        std::cout << "GetFileInfo info from all mds fail!" << std::endl;
        return -1;
    }
    if (response.has_statuscode() &&
                response.statuscode() == StatusCode::kOK) {
        fileInfo->CopyFrom(response.fileinfo());
        return 0;
    }
    std::cout << "GetFileInfo fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::ListDir(const std::string& dirName,
                       std::vector<FileInfo>* files) {
    if (!files) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    curve::mds::ListDirRequest request;
    curve::mds::ListDirResponse response;
    request.set_filename(dirName);
    FillUserInfo(&request);
    curve::mds::CurveFSService_Stub stub(&channel_);

    void (curve::mds::CurveFSService_Stub::*fp)(
                            google::protobuf::RpcController*,
                            const curve::mds::ListDirRequest*,
                            curve::mds::ListDirResponse*,
                            google::protobuf::Closure*);
    fp = &curve::mds::CurveFSService_Stub::ListDir;
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        std::cout << "ListDir from all mds fail!" << std::endl;
        return -1;
    }
    if (response.has_statuscode() &&
                response.statuscode() == StatusCode::kOK) {
        for (int i = 0; i < response.fileinfo_size(); ++i) {
            files->emplace_back(response.fileinfo(i));
        }
        return 0;
    }
    std::cout << "ListDir fail with errCode: "
              << response.statuscode() << std::endl;
        return -1;
    return -1;
}

GetSegmentRes MDSClient::GetSegmentInfo(const std::string& fileName,
                                         uint64_t offset,
                                         PageFileSegment* segment) {
    if (!segment) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return GetSegmentRes::kOtherError;
    }
    curve::mds::GetOrAllocateSegmentRequest request;
    curve::mds::GetOrAllocateSegmentResponse response;
    request.set_filename(fileName);
    request.set_offset(offset);
    request.set_allocateifnotexist(false);
    FillUserInfo(&request);
    curve::mds::CurveFSService_Stub stub(&channel_);

    void (curve::mds::CurveFSService_Stub::*fp)(
                            google::protobuf::RpcController*,
                            const curve::mds::GetOrAllocateSegmentRequest*,
                            curve::mds::GetOrAllocateSegmentResponse*,
                            google::protobuf::Closure*);
    fp = &curve::mds::CurveFSService_Stub::GetOrAllocateSegment;
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        std::cout << "GetOrAllocateSegment from all mds fail!" << std::endl;
        return GetSegmentRes::kOtherError;
    }
    if (response.has_statuscode()) {
        if (response.statuscode() == StatusCode::kOK) {
            segment->CopyFrom(response.pagefilesegment());
            return GetSegmentRes::kOK;
        } else if (response.statuscode() == StatusCode::kSegmentNotAllocated) {
            return GetSegmentRes::kSegmentNotAllocated;
        } else if (response.statuscode() == StatusCode::kFileNotExists) {
            return GetSegmentRes::kFileNotExists;
        }
    }
    return GetSegmentRes::kOtherError;
}

int MDSClient::DeleteFile(const std::string& fileName, bool forcedelete) {
    curve::mds::DeleteFileRequest request;
    curve::mds::DeleteFileResponse response;
    request.set_filename(fileName);
    request.set_forcedelete(forcedelete);
    FillUserInfo(&request);
    curve::mds::CurveFSService_Stub stub(&channel_);

    void (curve::mds::CurveFSService_Stub::*fp)(
                            google::protobuf::RpcController*,
                            const curve::mds::DeleteFileRequest*,
                            curve::mds::DeleteFileResponse*,
                            google::protobuf::Closure*);
    fp = &curve::mds::CurveFSService_Stub::DeleteFile;
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        std::cout << "DeleteFile from all mds fail!" << std::endl;
        return -1;
    }

    if (response.has_statuscode() &&
                (response.statuscode() == StatusCode::kOK ||
                 response.statuscode() == StatusCode::kFileNotExists)) {
        return 0;
    }
    std::cout << "DeleteFile fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::CreateFile(const std::string& fileName, uint64_t length) {
    curve::mds:: CreateFileRequest request;
    curve::mds::CreateFileResponse response;
    request.set_filename(fileName);
    request.set_filetype(curve::mds::FileType::INODE_PAGEFILE);
    request.set_filelength(length);
    FillUserInfo(&request);
    curve::mds::CurveFSService_Stub stub(&channel_);

    void (curve::mds::CurveFSService_Stub::*fp)(
                            google::protobuf::RpcController*,
                            const curve::mds::CreateFileRequest*,
                            curve::mds::CreateFileResponse*,
                            google::protobuf::Closure*);
    fp = &curve::mds::CurveFSService_Stub::CreateFile;
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        std::cout << "CreateFile from all mds fail!" << std::endl;
        return -1;
    }

    if (response.has_statuscode() &&
                response.statuscode() == StatusCode::kOK) {
        return 0;
    }
    std::cout << "CreateFile fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::GetChunkServerListInCopySets(const PoolIdType& logicalPoolId,
                                            const CopySetIdType& copysetId,
                                    std::vector<ChunkServerLocation>* csLocs) {
    if (!csLocs) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    curve::mds::topology::GetChunkServerListInCopySetsRequest request;
    curve::mds::topology::GetChunkServerListInCopySetsResponse response;
    request.set_logicalpoolid(logicalPoolId);
    request.add_copysetid(copysetId);
    curve::mds::topology::TopologyService_Stub stub(&channel_);

    void (curve::mds::topology::TopologyService_Stub::*fp)(
            google::protobuf::RpcController*,
            const curve::mds::topology::GetChunkServerListInCopySetsRequest*,
            curve::mds::topology::GetChunkServerListInCopySetsResponse*,
            google::protobuf::Closure*);
    fp = &curve::mds::topology::TopologyService_Stub::GetChunkServerListInCopySets;  // NOLINT
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        std::cout << "GetChunkServerListInCopySets from all mds fail!"
                  << std::endl;
        return -1;
    }

    if (response.has_statuscode() &&
                response.statuscode() == kTopoErrCodeSuccess) {
        for (int i = 0; i < response.csinfo(0).cslocs_size(); ++i) {
            auto location = response.csinfo(0).cslocs(i);
            csLocs->emplace_back(location);
        }
        return 0;
    }
    std::cout << "GetChunkServerListInCopySets fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::ListPhysicalPoolsInCluster(
                        std::vector<PhysicalPoolInfo>* pools) {
    if (!pools) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    curve::mds::topology::ListPhysicalPoolRequest request;
    curve::mds::topology::ListPhysicalPoolResponse response;
    curve::mds::topology::TopologyService_Stub stub(&channel_);

    void (curve::mds::topology::TopologyService_Stub::*fp)(
                google::protobuf::RpcController*,
                const curve::mds::topology::ListPhysicalPoolRequest*,
                curve::mds::topology::ListPhysicalPoolResponse*,
                google::protobuf::Closure*);
    fp = &curve::mds::topology::TopologyService_Stub::ListPhysicalPool;
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        std::cout << "ListPhysicalPool from all mds fail!"
                  << std::endl;
        return -1;
    }

    if (response.has_statuscode() &&
                response.statuscode() == kTopoErrCodeSuccess) {
        for (int i = 0; i < response.physicalpoolinfos_size(); ++i) {
            pools->emplace_back(response.physicalpoolinfos(i));
        }
        return 0;
    }
    std::cout << "ListPhysicalPool fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::ListLogicalPoolsInPhysicalPool(const PoolIdType& id,
                                      std::vector<LogicalPoolInfo>* pools) {
    if (!pools) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    curve::mds::topology::ListLogicalPoolRequest request;
    curve::mds::topology::ListLogicalPoolResponse response;
    request.set_physicalpoolid(id);
    curve::mds::topology::TopologyService_Stub stub(&channel_);

    void (curve::mds::topology::TopologyService_Stub::*fp)(
                google::protobuf::RpcController*,
                const curve::mds::topology::ListLogicalPoolRequest*,
                curve::mds::topology::ListLogicalPoolResponse*,
                google::protobuf::Closure*);
    fp = &curve::mds::topology::TopologyService_Stub::ListLogicalPool;
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        std::cout << "ListLogicalPool from all mds fail!"
                  << std::endl;
        return -1;
    }

    if (response.has_statuscode() &&
                response.statuscode() == kTopoErrCodeSuccess) {
        for (int i = 0; i < response.logicalpoolinfos_size(); ++i) {
            pools->emplace_back(response.logicalpoolinfos(i));
        }
        return 0;
    }
    std::cout << "ListLogicalPool fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::ListZoneInPhysicalPool(const PoolIdType& id,
                                      std::vector<ZoneInfo>* zones) {
    if (!zones) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    curve::mds::topology::ListPoolZoneRequest request;
    curve::mds::topology::ListPoolZoneResponse response;
    request.set_physicalpoolid(id);
    curve::mds::topology::TopologyService_Stub stub(&channel_);

    void (curve::mds::topology::TopologyService_Stub::*fp)(
                google::protobuf::RpcController*,
                const curve::mds::topology::ListPoolZoneRequest*,
                curve::mds::topology::ListPoolZoneResponse*,
                google::protobuf::Closure*);
    fp = &curve::mds::topology::TopologyService_Stub::ListPoolZone;
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        std::cout << "ListPoolZone from all mds fail!"
                  << std::endl;
        return -1;
    }

    if (response.has_statuscode() &&
                response.statuscode() == kTopoErrCodeSuccess) {
        for (int i = 0; i < response.zones_size(); ++i) {
            zones->emplace_back(response.zones(i));
        }
        return 0;
    }
    std::cout << "ListPoolZone fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::ListServersInZone(const ZoneIdType& id,
                                 std::vector<ServerInfo>* servers) {
    if (!servers) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    curve::mds::topology::ListZoneServerRequest request;
    curve::mds::topology::ListZoneServerResponse response;
    request.set_zoneid(id);
    curve::mds::topology::TopologyService_Stub stub(&channel_);

    void (curve::mds::topology::TopologyService_Stub::*fp)(
                google::protobuf::RpcController*,
                const curve::mds::topology::ListZoneServerRequest*,
                curve::mds::topology::ListZoneServerResponse*,
                google::protobuf::Closure*);
    fp = &curve::mds::topology::TopologyService_Stub::ListZoneServer;
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        std::cout << "ListZoneServer from all mds fail!"
                  << std::endl;
        return -1;
    }

    if (response.has_statuscode() &&
                response.statuscode() == kTopoErrCodeSuccess) {
        for (int i = 0; i < response.serverinfo_size(); ++i) {
            servers->emplace_back(response.serverinfo(i));
        }
        return 0;
    }
    std::cout << "ListZoneServer fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::ListChunkServersOnServer(const ServerIdType& id,
                                std::vector<ChunkServerInfo>* chunkservers) {
    if (!chunkservers) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    curve::mds::topology::ListChunkServerRequest request;
    request.set_serverid(id);
    return ListChunkServersOnServer(&request, chunkservers);
}
int MDSClient::ListChunkServersOnServer(const std::string& ip,
                                 std::vector<ChunkServerInfo>* chunkservers) {
    if (!chunkservers) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    curve::mds::topology::ListChunkServerRequest request;
    request.set_ip(ip);
    return ListChunkServersOnServer(&request, chunkservers);
}

int MDSClient::ListChunkServersOnServer(ListChunkServerRequest* request,
                                 std::vector<ChunkServerInfo>* chunkservers) {
    curve::mds::topology::ListChunkServerResponse response;
    curve::mds::topology::TopologyService_Stub stub(&channel_);

    void (curve::mds::topology::TopologyService_Stub::*fp)(
                google::protobuf::RpcController*,
                const curve::mds::topology::ListChunkServerRequest*,
                curve::mds::topology::ListChunkServerResponse*,
                google::protobuf::Closure*);
    fp = &curve::mds::topology::TopologyService_Stub::ListChunkServer;
    if (SendRpcToMds(request, &response, &stub, fp) != 0) {
        std::cout << "ListChunkServer from all mds fail!"
                  << std::endl;
        return -1;
    }

    if (response.has_statuscode() &&
                response.statuscode() == kTopoErrCodeSuccess) {
        for (int i = 0; i < response.chunkserverinfos_size(); ++i) {
            chunkservers->emplace_back(response.chunkserverinfos(i));
        }
        return 0;
    }
    std::cout << "ListChunkServer fail with errCode: "
              << response.statuscode() << std::endl;
        return -1;
}

int MDSClient::GetChunkServerInfo(const ChunkServerIdType& id,
                                  ChunkServerInfo* chunkserver) {
    if (!chunkserver) {
        std::cout << "The argument is a null pointer!"<< std::endl;
        return -1;
    }
    curve::mds::topology::GetChunkServerInfoRequest request;
    curve::mds::topology::GetChunkServerInfoResponse response;
    request.set_chunkserverid(id);
    return GetChunkServerInfo(&request, chunkserver);
}

int MDSClient::GetChunkServerInfo(const std::string& csAddr,
                                  ChunkServerInfo* chunkserver) {
    if (!chunkserver) {
        std::cout << "The argument is a null pointer!"<< std::endl;
        return -1;
    }
    curve::mds::topology::GetChunkServerInfoRequest request;
    curve::mds::topology::GetChunkServerInfoResponse response;
    if (!curve::common::NetCommon::CheckAddressValid(csAddr)) {
        std::cout << "chunkserver address invalid!" << std::endl;
        return -1;
    }
    std::vector<std::string> strs;
    curve::common::SplitString(csAddr, ":", &strs);
    std::string ip = strs[0];
    uint64_t port;
    curve::common::StringToUll(strs[1], &port);
    request.set_hostip(ip);
    request.set_port(port);
    return GetChunkServerInfo(&request, chunkserver);
}

int MDSClient::GetChunkServerInfo(GetChunkServerInfoRequest* request,
                                  ChunkServerInfo* chunkserver) {
    curve::mds::topology::GetChunkServerInfoResponse response;
    curve::mds::topology::TopologyService_Stub stub(&channel_);

    void (curve::mds::topology::TopologyService_Stub::*fp)(
                google::protobuf::RpcController*,
                const curve::mds::topology::GetChunkServerInfoRequest*,
                curve::mds::topology::GetChunkServerInfoResponse*,
                google::protobuf::Closure*);
    fp = &curve::mds::topology::TopologyService_Stub::GetChunkServer;
    if (SendRpcToMds(request, &response, &stub, fp) != 0) {
        std::cout << "GetChunkServer from all mds fail!"
                  << std::endl;
        return -1;
    }

    if (response.has_statuscode() &&
                response.statuscode() == kTopoErrCodeSuccess) {
        chunkserver->CopyFrom(response.chunkserverinfo());
        return 0;
    }
    std::cout << "GetChunkServer fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::GetCopySetsInChunkServer(const ChunkServerIdType& id,
                                 std::vector<CopysetInfo>* copysets) {
    if (!copysets) {
        std::cout << "The argument is a null pointer!"<< std::endl;
        return -1;
    }
    curve::mds::topology::GetCopySetsInChunkServerRequest request;
    curve::mds::topology::GetCopySetsInChunkServerResponse response;
    request.set_chunkserverid(id);
    return GetCopySetsInChunkServer(&request, copysets);
}

int MDSClient::GetCopySetsInChunkServer(const std::string& csAddr,
                                 std::vector<CopysetInfo>* copysets) {
    if (!copysets) {
        std::cout << "The argument is a null pointer!"<< std::endl;
        return -1;
    }
    curve::mds::topology::GetCopySetsInChunkServerRequest request;
    curve::mds::topology::GetCopySetsInChunkServerResponse response;
    if (!curve::common::NetCommon::CheckAddressValid(csAddr)) {
        std::cout << "chunkserver address invalid!" << std::endl;
        return -1;
    }
    std::vector<std::string> strs;
    curve::common::SplitString(csAddr, ":", &strs);
    std::string ip = strs[0];
    uint64_t port;
    curve::common::StringToUll(strs[1], &port);
    request.set_hostip(ip);
    request.set_port(port);
    return GetCopySetsInChunkServer(&request, copysets);
}

int MDSClient::GetCopySetsInChunkServer(
                            GetCopySetsInChunkServerRequest* request,
                            std::vector<CopysetInfo>* copysets) {
    curve::mds::topology::GetCopySetsInChunkServerResponse response;
    curve::mds::topology::TopologyService_Stub stub(&channel_);

    void (curve::mds::topology::TopologyService_Stub::*fp)(
                google::protobuf::RpcController*,
                const curve::mds::topology::GetCopySetsInChunkServerRequest*,
                curve::mds::topology::GetCopySetsInChunkServerResponse*,
                google::protobuf::Closure*);
    fp = &curve::mds::topology::TopologyService_Stub::GetCopySetsInChunkServer;
    if (SendRpcToMds(request, &response, &stub, fp) != 0) {
        std::cout << "GetCopySetsInChunkServer from all mds fail!"
                  << std::endl;
        return -1;
    }

    if (response.has_statuscode() &&
                response.statuscode() == kTopoErrCodeSuccess) {
        for (int i =0; i < response.copysetinfos_size(); ++i) {
            copysets->emplace_back(response.copysetinfos(i));
        }
        return 0;
    }
    std::cout << "GetCopySetsInChunkServer fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::ListServersInCluster(std::vector<ServerInfo>* servers) {
    if (!servers) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    // 先列出逻辑池
    std::vector<PhysicalPoolInfo> phyPools;
    if (ListPhysicalPoolsInCluster(&phyPools) != 0) {
        std::cout << "ListPhysicalPoolsInCluster fail!" << std::endl;
        return -1;
    }
    for (const auto& phyPool : phyPools) {
        std::vector<ZoneInfo> zones;
        if (ListZoneInPhysicalPool(phyPool.physicalpoolid(), &zones) != 0) {
            std::cout << "ListZoneInPhysicalPool fail, physicalPoolId: "
                      << phyPool.physicalpoolid() << std::endl;
            return -1;
        }
        for (const auto& zone : zones) {
            if (ListServersInZone(zone.zoneid(), servers) != 0) {
                std::cout << "ListServersInZone fail, zoneId :"
                          << zone.zoneid() << std::endl;
                return -1;
            }
        }
    }
    return 0;
}

int MDSClient::ListChunkServersInCluster(
                        std::vector<ChunkServerInfo>* chunkservers) {
    if (!chunkservers) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    std::vector<ServerInfo> servers;
    if (ListServersInCluster(&servers) != 0) {
        std::cout << "ListServersInCluster fail!" << std::endl;
        return -1;
    }
    for (const auto& server : servers) {
        if (ListChunkServersOnServer(server.serverid(), chunkservers) != 0) {
            std::cout << "ListChunkServersOnServer fail!" << std::endl;
            return -1;
        }
    }
    return 0;
}

int MDSClient::GetMetric(const std::string& metricName, uint64_t* value) {
    brpc::Channel httpChannel;
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_HTTP;
    int changeTimeLeft = mdsAddrVec_.size() - 1;
    while (changeTimeLeft >= 0) {
        int res = httpChannel.Init(mdsAddrVec_[currentMdsIndex_].c_str(),
                                                    &options);
        if (res != 0) {
            changeTimeLeft--;
            while (!ChangeMDServer() && changeTimeLeft > 0) {
                changeTimeLeft--;
            }
            continue;
        } else {
            brpc::Controller cntl;
            cntl.http_request().uri() = mdsAddrVec_[currentMdsIndex_] +
                                    "/vars/" + metricName;
            httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
            if (!cntl.Failed()) {
                std::vector<std::string> strs;
                curve::common::SplitString(
                    cntl.response_attachment().to_string(), ":", &strs);
                if (!curve::common::StringToUll(strs[1], value)) {
                    std::cout << "parse operator num fail!" << std::endl;
                    return -1;
                }
                return 0;
            }
            bool needRetry = (cntl.ErrorCode() != EHOSTDOWN &&
                              cntl.ErrorCode() != ETIMEDOUT &&
                              cntl.ErrorCode() != brpc::ELOGOFF &&
                              cntl.ErrorCode() != brpc::ERPCTIMEDOUT);
            uint64_t retryTimes = 0;
            while (needRetry && retryTimes < FLAGS_rpcRetryTimes) {
                cntl.Reset();
                cntl.http_request().uri() = mdsAddrVec_[currentMdsIndex_] +
                                    "/vars/" + metricName;
                httpChannel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
                if (cntl.Failed()) {
                    retryTimes++;
                    continue;
                }
            }
            if (needRetry) {
                std::cout << "Send RPC to mds fail, error content: "
                          << cntl.ErrorText() << std::endl;
                return -1;
            }
        }
    }
    std::cout << "GetMetric from all mds fail!"
              << std::endl;
    return -1;
}

bool MDSClient::ChangeMDServer() {
    currentMdsIndex_++;
    if (currentMdsIndex_ > mdsAddrVec_.size() - 1) {
        currentMdsIndex_ = 0;
    }
    if (channel_.Init(mdsAddrVec_[currentMdsIndex_].c_str(),
                                                nullptr) != 0) {
        return false;
    }
    return true;
}

std::string MDSClient::GetCurrentMds() {
    // 需要发送RPC确认一下，不然可能mds已经切换了
    curve::mds::GetFileInfoRequest request;
    curve::mds::GetFileInfoResponse response;
    request.set_filename("/");
    FillUserInfo(&request);
    curve::mds::CurveFSService_Stub stub(&channel_);

    void (curve::mds::CurveFSService_Stub::*fp)(
                            google::protobuf::RpcController*,
                            const curve::mds::GetFileInfoRequest*,
                            curve::mds::GetFileInfoResponse*,
                            google::protobuf::Closure*);
    fp = &curve::mds::CurveFSService_Stub::GetFileInfo;
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        return "";
    } else {
        return mdsAddrVec_[currentMdsIndex_];
    }
}

template <typename T, typename Request, typename Response>
int MDSClient::SendRpcToMds(Request* request, Response* response, T* obp,
                void (T::*func)(google::protobuf::RpcController*,
                            const Request*, Response*,
                            google::protobuf::Closure*)) {
    int changeTimeLeft = mdsAddrVec_.size() - 1;
    while (changeTimeLeft >= 0) {
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeout);
        (obp->*func)(&cntl, request, response, nullptr);
        if (!cntl.Failed()) {
            // 如果成功了，就返回0，对response的判断放到上一层
            return 0;
        }
        bool needRetry = (cntl.ErrorCode() != EHOSTDOWN &&
                          cntl.ErrorCode() != ETIMEDOUT &&
                          cntl.ErrorCode() != brpc::ELOGOFF);
        uint64_t retryTimes = 0;
        while (needRetry && retryTimes < FLAGS_rpcRetryTimes) {
            cntl.Reset();
            (obp->*func)(&cntl, request, response, nullptr);
            if (cntl.Failed()) {
                retryTimes++;
                continue;
            }
            return 0;
        }
        // 对于需要重试的错误，重试次数用完了还没成功就返回错误不切换
        // ERPCTIMEDOUT比较特殊，这种情况下，mds可能切换了也可能没切换，所以
        // 需要重试并且重试次数用完后切换
        // 只有不需要重试的，也就是mds不在线的才会去切换mds
        if (needRetry && cntl.ErrorCode() != brpc::ERPCTIMEDOUT) {
            std::cout << "Send RPC to mds fail, error content: "
                      << cntl.ErrorText() << std::endl;
            return -1;
        }
        changeTimeLeft--;
        while (!ChangeMDServer() && changeTimeLeft > 0) {
            changeTimeLeft--;
        }
    }
    return -1;
}

template <class T>
void MDSClient::FillUserInfo(T* request) {
    uint64_t date = curve::common::TimeUtility::GetTimeofDayUs();
    request->set_owner(userName_);
    request->set_date(date);

    if (!userName_.compare("root") &&
        password_.compare("")) {
        std::string str2sig = Authenticator::GetString2Signature(date,
                                                                 userName_);
        std::string sig = Authenticator::CalcString2Signature(str2sig,
                                                              password_);
        request->set_signature(sig);
    }
}
}  // namespace tool
}  // namespace curve

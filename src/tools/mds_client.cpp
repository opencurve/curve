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
 * Created Date: 2019-11-25
 * Author: charisu
 */
#include "src/tools/mds_client.h"

DECLARE_uint64(rpcTimeout);
DECLARE_uint64(rpcRetryTimes);

namespace curve {
namespace tool {

int MDSClient::Init(const std::string& mdsAddr) {
    return Init(mdsAddr, std::to_string(kDefaultMdsDummyPort));
}

int MDSClient::Init(const std::string& mdsAddr,
                    const std::string& dummyPort) {
    if (isInited_) {
        return 0;
    }
    // 初始化channel
    curve::common::SplitString(mdsAddr, ",", &mdsAddrVec_);
    if (mdsAddrVec_.empty()) {
        std::cout << "Split mds address fail!" << std::endl;
        return -1;
    }

    int res = InitDummyServerMap(dummyPort);
    if (res != 0) {
        std::cout << "init dummy server map fail!" << std::endl;
        return -1;
    }

    for (uint64_t i = 0; i < mdsAddrVec_.size(); ++i) {
        if (channel_.Init(mdsAddrVec_[i].c_str(), nullptr) != 0) {
            std::cout << "Init channel to " << mdsAddr << "fail!" << std::endl;
            continue;
        }
        // 寻找哪个mds存活
        curve::mds::topology::ListPhysicalPoolRequest request;
        curve::mds::topology::ListPhysicalPoolResponse response;
        curve::mds::topology::TopologyService_Stub stub(&channel_);
        brpc::Controller cntl;
        cntl.set_timeout_ms(FLAGS_rpcTimeout);
        stub.ListPhysicalPool(&cntl, &request, &response, nullptr);

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

int MDSClient::InitDummyServerMap(const std::string& dummyPort) {
    std::vector<std::string> dummyPortVec;
    curve::common::SplitString(dummyPort, ",", &dummyPortVec);
    if (dummyPortVec.size() == 0) {
        std::cout << "split dummy server fail!" << std::endl;
        return -1;
    }
    // 只指定了一个端口，对所有mds采用这个端口
    if (dummyPortVec.size() == 1) {
        for (uint64_t i = 0; i < mdsAddrVec_.size() - 1; ++i) {
            dummyPortVec.emplace_back(dummyPortVec[0]);
        }
    }

    if (dummyPortVec.size() != mdsAddrVec_.size()) {
        std::cout << "mds dummy port list must be correspond as"
                     " mds addr list" << std::endl;
        return -1;
    }

    for (uint64_t i = 0; i < mdsAddrVec_.size(); ++i) {
        std::vector<std::string> strs;
        curve::common::SplitString(mdsAddrVec_[i], ":", &strs);
        if (strs.size() != 2) {
            std::cout << "split mds addr fail!" << std::endl;
            return -1;
        }
        std::string dummyAddr = strs[0] + ":" + dummyPortVec[i];
        dummyServerMap_.emplace(mdsAddrVec_[i], dummyAddr);
    }
    return 0;
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

int MDSClient::GetAllocatedSize(const std::string& fileName,
                                uint64_t* allocSize,
                                uint64_t* physicalAllocSize) {
    if (!allocSize) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    curve::mds::GetAllocatedSizeRequest request;
    curve::mds::GetAllocatedSizeResponse response;
    request.set_filename(fileName);
    curve::mds::CurveFSService_Stub stub(&channel_);

    void (curve::mds::CurveFSService_Stub::*fp)(
                            google::protobuf::RpcController*,
                            const curve::mds::GetAllocatedSizeRequest*,
                            curve::mds::GetAllocatedSizeResponse*,
                            google::protobuf::Closure*);
    fp = &curve::mds::CurveFSService_Stub::GetAllocatedSize;
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        std::cout << "GetAllocatedSize info from all mds fail!" << std::endl;
        return -1;
    }
    if (response.statuscode() == StatusCode::kOK) {
        *allocSize = response.allocatedsize();
        if (physicalAllocSize) {
            *physicalAllocSize = response.physicalallocatedsize();
        }
        return 0;
    }
    std::cout << "GetAllocatedSize fail with errCode: "
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
                 response.statuscode() == StatusCode::kFileNotExists ||
                 response.statuscode() == StatusCode::kFileUnderDeleting)) {
        return 0;
    }
    std::cout << "DeleteFile fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::CreateFile(const std::string& fileName, uint64_t length) {
    curve::mds::CreateFileRequest request;
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

int MDSClient::ListClient(std::vector<std::string>* clientAddrs,
                          bool listClientsInRepo) {
    if (!clientAddrs) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    curve::mds::ListClientRequest request;
    curve::mds::ListClientResponse response;
    curve::mds::CurveFSService_Stub stub(&channel_);
    if (listClientsInRepo) {
        request.set_listallclient(true);
    }

    void (curve::mds::CurveFSService_Stub::*fp)(
                            google::protobuf::RpcController*,
                            const curve::mds::ListClientRequest*,
                            curve::mds::ListClientResponse*,
                            google::protobuf::Closure*);
    fp = &curve::mds::CurveFSService_Stub::ListClient;
    if (SendRpcToMds(&request, &response, &stub, fp) != 0) {
        std::cout << "ListClient from all mds fail!" << std::endl;
        return -1;
    }

    if (response.has_statuscode() &&
                response.statuscode() == StatusCode::kOK) {
        for (int i = 0; i < response.clientinfos_size(); ++i) {
            const auto& clientInfo = response.clientinfos(i);
            std::string clientAddr = clientInfo.ip() + ":" +
                                     std::to_string(clientInfo.port());
            clientAddrs->emplace_back(clientAddr);
        }
        return 0;
    }
    std::cout << "ListClient fail with errCode: "
              << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::GetChunkServerListInCopySet(const PoolIdType& logicalPoolId,
                                            const CopySetIdType& copysetId,
                                    std::vector<ChunkServerLocation>* csLocs) {
    if (!csLocs) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    std::vector<CopySetServerInfo> csServerInfos;
    int res = GetChunkServerListInCopySets(logicalPoolId,
                                                {copysetId}, &csServerInfos);
    if (res != 0) {
        std::cout << "GetChunkServerListInCopySets fail" << std::endl;
        return -1;
    }
    for (int i = 0; i < csServerInfos[0].cslocs_size(); ++i) {
        auto location = csServerInfos[0].cslocs(i);
        csLocs->emplace_back(location);
    }
    return 0;
}

int MDSClient::GetChunkServerListInCopySets(const PoolIdType& logicalPoolId,
                            const std::vector<CopySetIdType>& copysetIds,
                            std::vector<CopySetServerInfo>* csServerInfos) {
    if (!csServerInfos) {
        std::cout << "The argument is a null pointer!" << std::endl;
        return -1;
    }
    curve::mds::topology::GetChunkServerListInCopySetsRequest request;
    curve::mds::topology::GetChunkServerListInCopySetsResponse response;
    request.set_logicalpoolid(logicalPoolId);
    for (const auto& copysetId : copysetIds) {
        request.add_copysetid(copysetId);
    }
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
        for (int i = 0; i < response.csinfo_size(); ++i) {
            csServerInfos->emplace_back(response.csinfo(i));
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
            const auto& chunkserver = response.chunkserverinfos(i);
            // 跳过retired状态的chunkserver
            if (chunkserver.status() == ChunkServerStatus::RETIRED) {
                continue;
            }
            chunkservers->emplace_back(chunkserver);
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

int MDSClient::GetListenAddrFromDummyPort(const std::string& dummyAddr,
                                          std::string* listenAddr) {
    MetricRet res = metricClient_.GetConfValueFromMetric(dummyAddr,
                        kMdsListenAddrMetricName, listenAddr);
    if (res != MetricRet::kOK) {
        return -1;
    }
    return 0;
}

void MDSClient::GetMdsOnlineStatus(std::map<std::string, bool>* onlineStatus) {
    onlineStatus->clear();
    for (const auto item : dummyServerMap_) {
        std::string listenAddr;
        int res = GetListenAddrFromDummyPort(item.second, &listenAddr);
        // 如果获取到的监听地址与记录的mds地址不一致，也认为不在线
        if (res != 0 || listenAddr != item.first) {
            onlineStatus->emplace(item.first, false);
            continue;
        }
        onlineStatus->emplace(item.first, true);
    }
}

int MDSClient::GetMetric(const std::string& metricName, uint64_t* value) {
    std::string str;
    int res = GetMetric(metricName, &str);
    if (res != 0) {
        return -1;
    }
    if (!curve::common::StringToUll(str, value)) {
        std::cout << "parse metric as uint64_t fail!" << std::endl;
        return -1;
    }
    return 0;
}

int MDSClient::GetMetric(const std::string& metricName, std::string* value) {
    int changeTimeLeft = mdsAddrVec_.size() - 1;
    while (changeTimeLeft >= 0) {
        brpc::Controller cntl;
        MetricRet res = metricClient_.GetMetric(mdsAddrVec_[currentMdsIndex_],
                                         metricName, value);
        if (res == MetricRet::kOK) {
            return 0;
        }
        changeTimeLeft--;
        while (!ChangeMDServer() && changeTimeLeft > 0) {
            changeTimeLeft--;
        }
    }
    std::cout << "GetMetric " << metricName << " from all mds fail!"
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

std::vector<std::string> MDSClient::GetCurrentMds() {
    std::vector<std::string> leaderAddrs;
    for (const auto item : dummyServerMap_) {
        // 获取status来判断正在服务的地址
        std::string status;
        MetricRet ret = metricClient_.GetMetric(item.second,
                                        kMdsStatusMetricName, &status);
        if (ret != MetricRet::kOK) {
            std::cout << "Get status metric from " << item.second
                      << " fail" << std::endl;
            continue;
        }
        if (status == kMdsStatusLeader) {
            leaderAddrs.emplace_back(item.first);
        }
    }
    return leaderAddrs;
}

int MDSClient::RapidLeaderSchedule(PoolIdType lpoolId) {
    ::curve::mds::schedule::RapidLeaderScheduleRequst request;
    ::curve::mds::schedule::RapidLeaderScheduleResponse response;
    ::curve::mds::schedule::ScheduleService_Stub stub(&channel_);

    request.set_logicalpoolid(lpoolId);

    void (curve::mds::schedule::ScheduleService_Stub::*fp)(
        google::protobuf::RpcController*,
        const ::curve::mds::schedule::RapidLeaderScheduleRequst *,
        ::curve::mds::schedule::RapidLeaderScheduleResponse*,
        google::protobuf::Closure*);
    fp = &::curve::mds::schedule::ScheduleService_Stub::RapidLeaderSchedule;
    if (0 != SendRpcToMds(&request, &response, &stub, fp)) {
        std::cout << "RapidLeaderSchedule fail" << std::endl;
        return -1;
    }
    if (response.statuscode() ==
        ::curve::mds::schedule::kScheduleErrCodeSuccess) {
        std::cout << "RapidLeaderSchedule success" << std::endl;
        return 0;
    }
    std::cout << "RapidLeaderSchedule fail with errCode: "
        << response.statuscode() << std::endl;
    return -1;
}

int MDSClient::QueryChunkServerRecoverStatus(
    const std::vector<ChunkServerIdType>& cs,
    std::map<ChunkServerIdType, bool> *statusMap) {
    ::curve::mds::schedule::QueryChunkServerRecoverStatusRequest request;
    ::curve::mds::schedule::QueryChunkServerRecoverStatusResponse response;
    ::curve::mds::schedule::ScheduleService_Stub stub(&channel_);

    for (auto id : cs) {
        request.add_chunkserverid(id);
    }

    void (curve::mds::schedule::ScheduleService_Stub::*fp)(
        google::protobuf::RpcController*,
        const ::curve::mds::schedule::QueryChunkServerRecoverStatusRequest*,
        ::curve::mds::schedule::QueryChunkServerRecoverStatusResponse*,
        google::protobuf::Closure*);
    fp = &::curve::mds::schedule::ScheduleService_Stub::QueryChunkServerRecoverStatus; // NOLINT
    if (0 != SendRpcToMds(&request, &response, &stub, fp)) {
        std::cout << "QueryChunkServerRecoverStatus fail" << std::endl;
        return -1;
    }

    if (response.statuscode() ==
        ::curve::mds::schedule::kScheduleErrCodeSuccess) {
        for (auto it = response.recoverstatusmap().begin();
            it != response.recoverstatusmap().end(); ++it) {
            (*statusMap)[it->first] = it->second;
        }
        return 0;
    }
    std::cout << "QueryChunkServerRecoverStatus fail with errCode: "
        << response.statuscode() << std::endl;
    return -1;
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

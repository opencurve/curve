/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur Jun 15 2021
 * Author: lixiaocui
 */

#include "curvefs/src/client/rpcclient/mds_client.h"

#include <map>
#include <vector>
namespace curvefs {
namespace client {
namespace rpcclient {

FSStatusCode MdsClientImpl::Init(
    const ::curve::client::MetaServerOption& mdsOpt,
    MDSBaseClient* baseclient) {
    mdsOpt_ = mdsOpt;
    rpcexcutor_.SetOption(mdsOpt_.rpcRetryOpt);
    mdsbasecli_ = baseclient;

    std::ostringstream oss;
    std::for_each(mdsOpt_.rpcRetryOpt.addrs.begin(),
                  mdsOpt_.rpcRetryOpt.addrs.end(),
                  [&](const std::string& addr) { oss << " " << addr; });

    LOG(INFO) << "MDSClient init success, addresses:" << oss.str();
    return FSStatusCode::OK;
}

#define RPCTask                                                       \
    [&](int addrindex, uint64_t rpctimeoutMS, brpc::Channel* channel, \
        brpc::Controller* cntl) -> int

FSStatusCode MdsClientImpl::CreateFs(const std::string& fsName,
                                     uint64_t blockSize, const Volume& volume) {
    auto task = RPCTask {
        CreateFsResponse response;
        mdsbasecli_->CreateFs(fsName, blockSize, volume, &response, cntl,
                              channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "CreateFs Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        FSStatusCode ret = response.statuscode();
        LOG_IF(WARNING, ret != FSStatusCode::OK)
            << "CreateFs: fsname = " << fsName << ", blocksize = " << blockSize
            << ", volume = " << volume.DebugString() << ", errcode = " << ret
            << ", errmsg = " << FSStatusCode_Name(ret);
        return ret;
    };
    return ReturnError(rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS));
}

FSStatusCode MdsClientImpl::CreateFsS3(const std::string& fsName,
                                       uint64_t blockSize,
                                       const S3Info& s3Info) {
    auto task = RPCTask {
        CreateFsResponse response;
        mdsbasecli_->CreateFsS3(fsName, blockSize, s3Info, &response, cntl,
                                channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "CreateFs Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        FSStatusCode ret = response.statuscode();
        LOG_IF(WARNING, ret != FSStatusCode::OK)
            << "CreateFs: fsname = " << fsName << ", blocksize = " << blockSize
            << ", errcode = " << ret << ", errmsg = " << FSStatusCode_Name(ret);

        return ret;
    };
    return ReturnError(rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS));
}

FSStatusCode MdsClientImpl::DeleteFs(const std::string& fsName) {
    auto task = RPCTask {
        DeleteFsResponse response;
        mdsbasecli_->DeleteFs(fsName, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "DeleteFs Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        FSStatusCode ret = response.statuscode();
        LOG_IF(WARNING, ret != FSStatusCode::OK)
            << "DeleteFs: fsname = " << fsName << ", errcode = " << ret
            << ", errmsg = " << FSStatusCode_Name(ret);
        return ret;
    };
    return ReturnError(rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS));
}

FSStatusCode MdsClientImpl::MountFs(const std::string& fsName,
                                    const std::string& mountPt,
                                    FsInfo* fsInfo) {
    auto task = RPCTask {
        MountFsResponse response;
        mdsbasecli_->MountFs(fsName, mountPt, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "MountFs Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        FSStatusCode ret = response.statuscode();
        if (ret != FSStatusCode::OK) {
            LOG(WARNING) << "MountFs: fsname = " << fsName
                         << ", mountPt = " << mountPt << ", errcode = " << ret
                         << ", errmsg = " << FSStatusCode_Name(ret);
        } else if (response.has_fsinfo()) {
            fsInfo->CopyFrom(response.fsinfo());
        }
        return ret;
    };
    return ReturnError(rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS));
}

FSStatusCode MdsClientImpl::UmountFs(const std::string& fsName,
                                     const std::string& mountPt) {
    auto task = RPCTask {
        UmountFsResponse response;
        mdsbasecli_->UmountFs(fsName, mountPt, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "UmountFs Failed, errorcode = " << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        FSStatusCode ret = response.statuscode();
        LOG_IF(WARNING, ret != FSStatusCode::OK)
            << "UmountFs: fsname = " << fsName << ", mountPt = " << mountPt
            << ", errcode = " << ret << ", errmsg = " << FSStatusCode_Name(ret);
        return ret;
    };
    return ReturnError(rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS));
}

FSStatusCode MdsClientImpl::GetFsInfo(const std::string& fsName,
                                      FsInfo* fsInfo) {
    auto task = RPCTask {
        GetFsInfoResponse response;
        mdsbasecli_->GetFsInfo(fsName, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "GetFsInfo Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        FSStatusCode ret = response.statuscode();
        if (ret != FSStatusCode::OK) {
            LOG(WARNING) << "GetFsInfo: fsname = " << fsName
                         << ", errcode = " << ret
                         << ", errmsg = " << FSStatusCode_Name(ret);
        } else if (response.has_fsinfo()) {
            fsInfo->CopyFrom(response.fsinfo());
        }

        return ret;
    };
    return ReturnError(rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS));
}

FSStatusCode MdsClientImpl::GetFsInfo(uint32_t fsId, FsInfo* fsInfo) {
    auto task = RPCTask {
        GetFsInfoResponse response;
        mdsbasecli_->GetFsInfo(fsId, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "GetFsInfo Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        FSStatusCode ret = response.statuscode();
        if (ret != FSStatusCode::OK) {
            LOG(WARNING) << "GetFsInfo: fsid = " << fsId
                         << ", errcode = " << ret
                         << ", errmsg = " << FSStatusCode_Name(ret);
        } else if (response.has_fsinfo()) {
            fsInfo->CopyFrom(response.fsinfo());
        }
        return ret;
    };
    return ReturnError(rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS));
}

TopoStatusCode MdsClientImpl::CommitTx(
    const std::vector<PartitionTxId>& txIds) {
    auto task = RPCTask {
        CommitTxResponse response;
        mdsbasecli_->CommitTx(txIds, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "CommitTx failed, errorCode = " << cntl->ErrorCode()
                         << ", errorText =" << cntl->ErrorText()
                         << ", logId = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        auto rc = response.statuscode();
        if (rc != TopoStatusCode::TOPO_OK) {
            LOG(WARNING) << "CommitTx: retCode = " << rc
                         << ", message = " << TopoStatusCode_Name(rc);
        }
        return rc;
    };
    // NOTE: retry until success
    auto rc = rpcexcutor_.DoRPCTask(task, 0);
    return static_cast<TopoStatusCode>(rc);
}

bool MdsClientImpl::GetMetaServerInfo(
    const PeerAddr& addr, CopysetPeerInfo<MetaserverID>* metaserverInfo) {
    std::vector<std::string> strs;
    curve::common::SplitString(addr.ToString(), ":", &strs);
    const std::string& ip = strs[0];
    uint64_t port;
    ::curve::common::StringToUll(strs[1], &port);

    auto task = RPCTask {
        GetMetaServerInfoResponse response;
        mdsbasecli_->GetMetaServerInfo(port, ip, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "GetMetaServerInfo Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        // TODO(lixiaocui): @wanghai 这里uint32返回的是什么
        uint32_t ret = response.statuscode();
        if (ret != 0) {
            LOG(WARNING) << "GetMetaServerInfo: ip= " << ip
                         << ", port= " << port << ", errcode = " << ret;
        } else {
            const auto& info = response.metaserverinfo();
            MetaserverID metaserverID = info.metaserverid();
            std::string internalIp = info.hostip();
            std::string externalIp = internalIp;
            if (info.has_externalip()) {
                externalIp = info.externalip();
            }
            uint32_t port = info.port();
            butil::EndPoint internal;
            butil::str2endpoint(internalIp.c_str(), port, &internal);
            butil::EndPoint external;
            butil::str2endpoint(externalIp.c_str(), port, &external);
            *metaserverInfo = CopysetPeerInfo<MetaserverID>(
                metaserverID, PeerAddr(internal), PeerAddr(external));
        }

        return ret;
    };
    return 0 == rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS);
}

bool MdsClientImpl::GetMetaServerListInCopysets(
    const LogicPoolID& logicalpooid, const std::vector<CopysetID>& copysetidvec,
    std::vector<CopysetInfo<MetaserverID>>* cpinfoVec) {
    auto task = RPCTask {
        GetMetaServerListInCopySetsResponse response;
        mdsbasecli_->GetMetaServerListInCopysets(logicalpooid, copysetidvec,
                                                 &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "get metaserver list from mds failed, error is "
                         << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        int csinfonum = response.csinfo_size();
        for (int i = 0; i < csinfonum; i++) {
            CopysetInfo<MetaserverID> copysetseverl;
            ::curvefs::mds::topology::CopySetServerInfo info =
                response.csinfo(i);

            copysetseverl.lpid_ = logicalpooid;
            copysetseverl.cpid_ = info.copysetid();
            int cslocsNum = info.cslocs_size();
            for (int j = 0; j < cslocsNum; j++) {
                CopysetPeerInfo<MetaserverID> csinfo;
                ::curvefs::mds::topology::MetaServerLocation csl =
                    info.cslocs(j);
                uint16_t port = csl.port();
                std::string internalIp = csl.hostip();
                csinfo.peerID = csl.metaserverid();
                std::string externalIp = internalIp;
                if (csl.has_externalip()) {
                    externalIp = csl.externalip();
                }

                butil::EndPoint internal;
                butil::str2endpoint(internalIp.c_str(), port, &internal);
                butil::EndPoint external;
                butil::str2endpoint(externalIp.c_str(), port, &external);
                csinfo.internalAddr = PeerAddr(internal);
                csinfo.externalAddr = PeerAddr(external);
                copysetseverl.AddCopysetPeerInfo(csinfo);
            }
            cpinfoVec->push_back(copysetseverl);
        }
        TopoStatusCode ret = response.statuscode();
        LOG_IF(WARNING, TopoStatusCode::TOPO_OK != 0)
            << "GetMetaServerList failed"
            << ", errocde = " << response.statuscode()
            << ", log id = " << cntl->log_id();
        return ret;
    };

    return 0 == rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS);
}

bool MdsClientImpl::CreatePartition(
    uint32_t fsID, uint32_t count, std::vector<PartitionInfo>* partitionInfos) {
    auto task = RPCTask {
        CreatePartitionResponse response;
        mdsbasecli_->CreatePartition(fsID, count, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "CreatePartition from mds failed, error is "
                         << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        TopoStatusCode ret = response.statuscode();
        if (ret != TopoStatusCode::TOPO_OK) {
            LOG(WARNING) << "CreatePartition: fsID = " << fsID
                         << ", count = " << count << ", errcode = " << ret
                         << ", errmsg = " << TopoStatusCode_Name(ret);
            return ret;
        }

        int partitionNum = response.partitioninfolist_size();
        if (partitionNum == 0) {
            LOG(ERROR) << "CreatePartition: fsID = " << fsID
                       << ", count = " << count << ", errcode = " << ret
                       << ", errmsg = " << TopoStatusCode_Name(ret)
                       << ", but no partition info returns";
            return TopoStatusCode::TOPO_CREATE_PARTITION_FAIL;
        }

        partitionInfos->clear();
        for (int i = 0; i < partitionNum; i++) {
            partitionInfos->push_back(response.partitioninfolist(i));
        }

        return TopoStatusCode::TOPO_OK;
    };

    return 0 == rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS);
}

bool MdsClientImpl::GetCopysetOfPartitions(
    const std::vector<uint32_t>& partitionIDList,
    std::map<uint32_t, Copyset>* copysetMap) {
    auto task = RPCTask {
        GetCopysetOfPartitionResponse response;
        mdsbasecli_->GetCopysetOfPartitions(partitionIDList, &response, cntl,
                                            channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "GetCopysetOfPartition from mds failed, error is "
                         << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        TopoStatusCode ret = response.statuscode();
        if (ret != TopoStatusCode::TOPO_OK) {
            LOG(WARNING) << "GetCopysetOfPartition: errcode = " << ret
                         << ", errmsg = " << TopoStatusCode_Name(ret);
            return ret;
        }

        int size = response.copysetmap_size();
        if (size == 0) {
            LOG(WARNING) << "GetCopysetOfPartition: errcode = " << ret
                         << ", errmsg = " << TopoStatusCode_Name(ret)
                         << ", but no copyset returns";
            return TopoStatusCode::TOPO_INTERNAL_ERROR;
        }

        copysetMap->clear();
        for (auto it : response.copysetmap()) {
            CopysetPeerInfo<MetaserverID> csinfo;
            copysetMap->emplace(it.first, it.second);
        }

        return TopoStatusCode::TOPO_OK;
    };

    return 0 == rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS);
}

bool MdsClientImpl::ListPartition(uint32_t fsID,
                                  std::vector<PartitionInfo>* partitionInfos) {
    auto task = RPCTask {
        ListPartitionResponse response;
        mdsbasecli_->ListPartition(fsID, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "ListPartition from mds failed, error is "
                         << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        TopoStatusCode ret = response.statuscode();
        if (ret != TopoStatusCode::TOPO_OK) {
            LOG(WARNING) << "ListPartition: fsID = " << fsID
                         << ", errcode = " << ret
                         << ", errmsg = " << TopoStatusCode_Name(ret);
            return ret;
        }

        partitionInfos->clear();
        // when fs is creating and mds exit at the same time,
        // this may cause this fs has no partition
        int partitionNum = response.partitioninfolist_size();
        for (int i = 0; i < partitionNum; i++) {
            partitionInfos->push_back(response.partitioninfolist(i));
        }

        return TopoStatusCode::TOPO_OK;
    };

    return 0 == rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS);
}

FSStatusCode MdsClientImpl::AllocS3ChunkId(uint32_t fsId, uint64_t* chunkId) {
    auto task = RPCTask {
        AllocateS3ChunkResponse response;
        mdsbasecli_->AllocS3ChunkId(fsId, &response, cntl, channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "AllocS3ChunkId Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return -cntl->ErrorCode();
        }

        FSStatusCode ret = response.statuscode();
        if (ret != FSStatusCode::OK) {
            LOG(WARNING) << "AllocS3ChunkId: fsid = " << fsId
                         << ", errcode = " << ret
                         << ", errmsg = " << FSStatusCode_Name(ret);
        } else if (response.has_chunkid()) {
            *chunkId = response.chunkid();
        }

        return ret;
    };
    return ReturnError(rpcexcutor_.DoRPCTask(task, mdsOpt_.mdsMaxRetryMS));
}

FSStatusCode MdsClientImpl::ReturnError(int retcode) {
    // rpc error convert to FSStatusCode::RPC_ERROR
    if (retcode < 0) {
        return FSStatusCode::RPC_ERROR;
    }

    // logic error
    return static_cast<FSStatusCode>(retcode);
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

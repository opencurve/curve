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
 * @Project: curve
 * @Date: 2021-06-24 11:21:36
 * @Author: chenwei
 */

#include "curvefs/src/mds/metaserverclient/metaserver_client.h"

using curvefs::metaserver::CreateRootInodeRequest;
using curvefs::metaserver::CreateRootInodeResponse;
using curvefs::metaserver::DeleteInodeRequest;
using curvefs::metaserver::DeleteInodeResponse;
using curvefs::metaserver::MetaServerService_Stub;
using curvefs::metaserver::MetaStatusCode;
using curvefs::metaserver::copyset::COPYSET_OP_STATUS;
using curvefs::metaserver::copyset::CopysetService_Stub;

namespace curvefs {
namespace mds {
bool MetaserverClient::Init() {
    LOG(INFO) << "MetaserverClient Inited";
    inited_ = true;
    return true;
}

void MetaserverClient::Uninit() { inited_ = false; }

FSStatusCode MetaserverClient::CreateRootInode(uint32_t fsId, uint32_t poolId,
                    uint32_t copysetId, uint32_t partitionId, uint32_t uid,
                    uint32_t gid, uint32_t mode, const std::string &leader) {
    if (!inited_) {
        LOG(ERROR) << "MetaserverClient not Init, init first";
        return FSStatusCode::METASERVER_CLIENT_NOT_INITED;
    }
    CreateRootInodeRequest request;
    CreateRootInodeResponse response;

    if (channel_.Init(leader.c_str(), nullptr) != 0) {
        LOG(ERROR) << "Init channel to metaserver: " << leader
                   << " failed!";
        return FSStatusCode::RPC_ERROR;;
    }
    brpc::Controller cntl;
    cntl.set_timeout_ms(options_.rpcTimeoutMs);

    MetaServerService_Stub stub(&channel_);
    request.set_poolid(poolId);
    request.set_copysetid(copysetId);
    request.set_partitionid(partitionId);
    request.set_fsid(fsId);
    request.set_uid(uid);
    request.set_gid(gid);
    request.set_mode(mode);

    stub.CreateRootInode(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "CreateInode failed, fsId = " << fsId << ", uid = " << uid
                   << ", gid = " << gid << ", mode =" << mode
                   << ", Rpc error = " << cntl.ErrorText();
        return FSStatusCode::RPC_ERROR;
    }

    switch (response.statuscode()) {
        case MetaStatusCode::OK:
            return FSStatusCode::OK;
        case MetaStatusCode::INODE_EXIST:
            return FSStatusCode::INODE_EXIST;
        default:
            LOG(ERROR) << "CreateInode failed, fsId = " << fsId
                       << ", uid = " << uid << ", gid = " << gid
                       << ", mode =" << mode << ", ret = "
                       << FSStatusCode_Name(
                              FSStatusCode::INSERT_ROOT_INODE_ERROR);
            return FSStatusCode::INSERT_ROOT_INODE_ERROR;
    }
}

FSStatusCode MetaserverClient::DeleteInode(uint32_t fsId, uint64_t inodeId) {
    if (!inited_) {
        LOG(ERROR) << "MetaserverClient not Init, init first";
        return FSStatusCode::METASERVER_CLIENT_NOT_INITED;
    }

    DeleteInodeRequest request;
    DeleteInodeResponse response;

    brpc::Controller cntl;
    cntl.set_timeout_ms(options_.rpcTimeoutMs);

    if (channel_.Init(options_.metaserverAddr.c_str(), nullptr) != 0) {
        LOG(ERROR) << "Init channel to metaserver: " << options_.metaserverAddr
                   << " failed!";
        return FSStatusCode::RPC_ERROR;
    }
    MetaServerService_Stub stub(&channel_);
    // TODO(cw123): add partiton
    request.set_poolid(0);
    request.set_copysetid(0);
    request.set_partitionid(0);
    request.set_fsid(fsId);
    request.set_inodeid(inodeId);
    // TODO(@威姐): 适配新的proto
    request.set_copysetid(1);
    request.set_poolid(1);
    request.set_partitionid(1);

    stub.DeleteInode(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "DeleteInode failed"
                   << ", fsId = " << fsId << ", inodeId = " << inodeId
                   << ", Rpc error = " << cntl.ErrorText();
        return FSStatusCode::RPC_ERROR;
    }

    if (response.statuscode() != MetaStatusCode::OK) {
        LOG(ERROR) << "DeleteInode failed, fsId = " << fsId
                   << ", inodeId = " << inodeId << ", ret = "
                   << FSStatusCode_Name(FSStatusCode::DELETE_INODE_ERROR);
        return FSStatusCode::DELETE_INODE_ERROR;
    }

    return FSStatusCode::OK;
}

FSStatusCode MetaserverClient::GetLeader(uint32_t poolId, uint32_t copysetId,
                                         const std::set<std::string> &addrs,
                                         std::string *leader) {
    GetLeaderRequest2 request;
    GetLeaderResponse2 response;
    request.set_poolid(poolId);
    request.set_copysetid(copysetId);

    for (std::string item : addrs) {
        if (channel_.Init(item.c_str(), nullptr) != 0) {
            LOG(ERROR) << "Init channel to metaserver: " << item
                       << " failed!";
            continue;
        }

        brpc::Controller cntl;
        cntl.set_timeout_ms(options_.rpcTimeoutMs);
        CliService2_Stub stub(&channel_);
        stub.GetLeader(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            LOG(WARNING) << "GetLeader failed"
                         << ", poolid = " << poolId
                         << ", copysetId = " << copysetId
                         << ", Rpc error = " << cntl.ErrorText();
            continue;
        }
        if (response.has_leader()) {
            *leader = response.leader().address();
            return FSStatusCode::OK;
        }
    }
    return FSStatusCode::NOT_FOUND;
}

FSStatusCode MetaserverClient::CreatePartition(uint32_t fsId, uint32_t poolId,
                                    uint32_t copysetId, uint32_t partitionId,
                                    uint64_t idStart, uint64_t idEnd,
                                    const std::string &addr) {
    curvefs::metaserver::CreatePartitionRequest request;
    curvefs::metaserver::CreatePartitionResponse response;
    PartitionInfo *partition = request.mutable_partition();
    partition->set_fsid(fsId);
    partition->set_poolid(poolId);
    partition->set_copysetid(copysetId);
    partition->set_partitionid(partitionId);
    partition->set_start(idStart);
    partition->set_end(idEnd);
    partition->set_txid(0);

    if (channel_.Init(addr.c_str(), nullptr) != 0) {
        LOG(ERROR) << "Init channel to metaserver: " << addr
                   << " failed!";
        return FSStatusCode::RPC_ERROR;
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(options_.rpcTimeoutMs);
    MetaServerService_Stub stub(&channel_);
    stub.CreatePartition(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        LOG(ERROR) << "Create partition failed"
                   << ", Rpc error = " << cntl.ErrorText();
        return FSStatusCode::RPC_ERROR;
    }
    if (response.statuscode() != MetaStatusCode::OK) {
        LOG(ERROR) << "Create partition failed, request = "
                   << request.ShortDebugString();
        return FSStatusCode::CREATE_PARTITION_ERROR;
    }
    return FSStatusCode::OK;
}

FSStatusCode MetaserverClient::CreateCopySet(uint32_t poolId,
    std::set<uint32_t> copysetIds, const std::set<std::string> &addrs) {
    CreateCopysetRequest request;
    CreateCopysetResponse response;
    for (auto id : copysetIds) {
        auto copyset = request.add_copysets();
        copyset->set_poolid(poolId);
        copyset->set_copysetid(id);
        for (auto item : addrs) {
            copyset->add_peers()->set_address(item + std::to_string(0));
        }
    }

    for (std::string item : addrs) {
        if (channel_.Init(item.c_str(), nullptr) != 0) {
            LOG(ERROR) << "Init channel to metaserver: " << item
                       << " failed!";
            return FSStatusCode::RPC_ERROR;
        }

        brpc::Controller cntl;
        cntl.set_timeout_ms(options_.rpcTimeoutMs);
        CopysetService_Stub stub(&channel_);
        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            LOG(ERROR) << "Create copyset failed"
                       << ", Rpc error = " << cntl.ErrorText();
            return FSStatusCode::RPC_ERROR;
        }
        if (response.status() != COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS) {
            LOG(ERROR) << "Create copyset failed."
                       << " from " << cntl.remote_side()
                       << " to " << cntl.local_side()
                       << " request = " << request.ShortDebugString()
                       << " error code = " << response.status();
            return FSStatusCode::CREATE_COPYSET_ERROR;
        }
    }
    return FSStatusCode::OK;
}

}  // namespace mds
}  // namespace curvefs

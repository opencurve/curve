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
#include <bthread/bthread.h>

namespace curvefs {
namespace mds {

using curvefs::metaserver::CreateRootInodeRequest;
using curvefs::metaserver::CreateRootInodeResponse;
using curvefs::metaserver::DeleteInodeRequest;
using curvefs::metaserver::DeleteInodeResponse;
using curvefs::metaserver::MetaServerService_Stub;
using curvefs::metaserver::MetaStatusCode;
using curvefs::metaserver::copyset::COPYSET_OP_STATUS;
using curvefs::metaserver::copyset::CopysetService_Stub;
using curvefs::mds::topology::SplitPeerId;
using curvefs::mds::topology::BuildPeerIdWithAddr;

template <typename T, typename Request, typename Response>
FSStatusCode MetaserverClient::SendRpc2MetaServer(Request* request,
    Response* response, const LeaderCtx &ctx,
    void (T::*func)(google::protobuf::RpcController*,
                    const Request*, Response*,
                    google::protobuf::Closure*)) {
    bool needRetry = true;
    bool refreshLeader = true;
    uint32_t maxRetry = options_.rpcRetryTimes;

    std::string leader;
    auto poolId = ctx.poolId;
    auto copysetId = ctx.copysetId;
    brpc::Controller cntl;
    do {
        if (refreshLeader) {
            auto ret = GetLeader(ctx, &leader);
            if (ret != FSStatusCode::OK) {
                LOG(ERROR) << "Get leader fail"
                           << ", poolId = " << poolId
                           << ", copysetId = " << copysetId;
                return ret;
            }
            if (channel_.Init(leader.c_str(), nullptr) != 0) {
                LOG(ERROR) << "Init channel to metaserver: " << leader
                           << " failed!";
                return FSStatusCode::RPC_ERROR;
            }
        }

        cntl.Reset();
        cntl.set_timeout_ms(options_.rpcTimeoutMs);
        MetaServerService_Stub stub(&channel_);
        (stub.*func)(&cntl, request, response, nullptr);
        if (cntl.Failed()) {
            needRetry = true;
            if (cntl.ErrorCode() == EHOSTDOWN ||
                cntl.ErrorCode() == brpc::ELOGOFF) {
                    refreshLeader = true;
                } else {
                    refreshLeader = false;
                }
        } else {
            switch (response->statuscode()) {
                case MetaStatusCode::OVERLOAD:
                    needRetry = true;
                    refreshLeader = false;
                    break;
                case MetaStatusCode::REDIRECTED:
                    needRetry = true;
                    refreshLeader = true;
                    break;
                default:
                    return FSStatusCode::UNKNOWN_ERROR;
            }
        }
        maxRetry--;
    } while (maxRetry > 0);

    if (cntl.Failed()) {
        return FSStatusCode::RPC_ERROR;
    } else {
        return FSStatusCode::UNKNOWN_ERROR;
    }
}

FSStatusCode MetaserverClient::GetLeader(const LeaderCtx &ctx,
                                         std::string *leader) {
    GetLeaderRequest2 request;
    GetLeaderResponse2 response;
    request.set_poolid(ctx.poolId);
    request.set_copysetid(ctx.copysetId);

    for (const std::string &item : ctx.addrs) {
        if (channel_.Init(item.c_str(), nullptr) != 0) {
            LOG(ERROR) << "Init channel to metaserver: " << item
                       << " failed!";
            continue;
        }

        brpc::Controller cntl;
        cntl.set_timeout_ms(options_.rpcTimeoutMs);
        CliService2_Stub stub(&channel_);
        stub.GetLeader(&cntl, &request, &response, nullptr);

        uint32_t maxRetry = options_.rpcRetryTimes;
        while (cntl.Failed() && maxRetry > 0) {
            maxRetry--;
            bthread_usleep(options_.rpcRetryIntervalUs);
            cntl.Reset();
            cntl.set_timeout_ms(options_.rpcTimeoutMs);
            stub.GetLeader(&cntl, &request, &response, nullptr);
        }
        if (cntl.Failed()) {
            LOG(WARNING) << "GetLeader failed"
                         << ", poolid = " << ctx.poolId
                         << ", copysetId = " << ctx.copysetId
                         << ", Rpc error = " << cntl.ErrorText();
            continue;
        }
        if (response.has_leader()) {
            std::string ip;
            uint32_t port;
            SplitPeerId(response.leader().address(), &ip, &port);
            *leader = ip + ":" + std::to_string(port);
            return FSStatusCode::OK;
        }
    }
    return FSStatusCode::NOT_FOUND;
}

FSStatusCode MetaserverClient::CreateRootInode(uint32_t fsId, uint32_t poolId,
                    uint32_t copysetId, uint32_t partitionId, uint32_t uid,
                    uint32_t gid, uint32_t mode,
                    const std::set<std::string> &addrs) {
    CreateRootInodeRequest request;
    CreateRootInodeResponse response;
    request.set_poolid(poolId);
    request.set_copysetid(copysetId);
    request.set_partitionid(partitionId);
    request.set_fsid(fsId);
    request.set_uid(uid);
    request.set_gid(gid);
    request.set_mode(mode);

    auto fp = &MetaServerService_Stub::CreateRootInode;
    LeaderCtx ctx;
    ctx.addrs = addrs;
    ctx.poolId = request.poolid();
    ctx.copysetId = request.copysetid();
    auto ret = SendRpc2MetaServer(&request, &response, ctx, fp);

    if (FSStatusCode::RPC_ERROR == ret) {
        LOG(ERROR) << "CreateInode failed, rpc error. request = "
                   << request.ShortDebugString();
        return ret;
    } else if (FSStatusCode::NOT_FOUND == ret) {
        LOG(ERROR) << "CreateInode failed, get leader failed. request = "
                   << request.ShortDebugString();
        return ret;
    } else {
        switch (response.statuscode()) {
            case MetaStatusCode::OK:
                return FSStatusCode::OK;
            case MetaStatusCode::INODE_EXIST:
                LOG(ERROR) << "CreateInode failed, Inode exist.";
                return FSStatusCode::INODE_EXIST;
            default:
                LOG(ERROR) << "CreateInode failed, request = "
                        << request.ShortDebugString()
                        << ", response statuscode = " << response.statuscode();
                return FSStatusCode::INSERT_ROOT_INODE_ERROR;
        }
    }
}

FSStatusCode MetaserverClient::DeleteInode(uint32_t fsId, uint64_t inodeId) {
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

FSStatusCode MetaserverClient::CreatePartition(uint32_t fsId, uint32_t poolId,
                                    uint32_t copysetId, uint32_t partitionId,
                                    uint64_t idStart, uint64_t idEnd,
                                    const std::set<std::string> &addrs) {
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
    partition->set_status(PartitionStatus::READWRITE);

    auto fp = &MetaServerService_Stub::CreatePartition;
    LeaderCtx ctx;
    ctx.addrs = addrs;
    ctx.poolId = request.partition().poolid();
    ctx.copysetId = request.partition().copysetid();
    auto ret = SendRpc2MetaServer(&request, &response, ctx, fp);

    if (FSStatusCode::RPC_ERROR == ret) {
        LOG(ERROR) << "CreatePartition failed, rpc error. request = "
                   << request.ShortDebugString();
        return ret;
    } else if (FSStatusCode::NOT_FOUND == ret) {
        LOG(ERROR) << "CreatePartition failed, get leader failed. request = "
                   << request.ShortDebugString();
        return ret;
    } else {
        switch (response.statuscode()) {
            case MetaStatusCode::OK:
                return FSStatusCode::OK;
            case MetaStatusCode::PARTITION_EXIST:
                LOG(ERROR) << "CreatePartition failed, partition exist.";
                return FSStatusCode::PARTITION_EXIST;
            default:
                LOG(ERROR) << "CreatePartition failed, request = "
                        << request.ShortDebugString()
                        << ", response statuscode = " << response.statuscode();
                return FSStatusCode::CREATE_PARTITION_ERROR;
        }
    }
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
            copyset->add_peers()->set_address(BuildPeerIdWithAddr(item));
        }
    }

    for (const std::string &item : addrs) {
        if (channel_.Init(item.c_str(), nullptr) != 0) {
            LOG(ERROR) << "Init channel to metaserver: " << item
                       << " failed!";
            return FSStatusCode::RPC_ERROR;
        }

        brpc::Controller cntl;
        cntl.set_timeout_ms(options_.rpcTimeoutMs);
        CopysetService_Stub stub(&channel_);
        stub.CreateCopysetNode(&cntl, &request, &response, nullptr);

        uint32_t maxRetry = options_.rpcRetryTimes;
        while (cntl.Failed() && maxRetry > 0) {
            maxRetry--;
            bthread_usleep(options_.rpcRetryIntervalUs);
            cntl.Reset();
            cntl.set_timeout_ms(options_.rpcTimeoutMs);
            stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
        }
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

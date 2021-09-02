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
 * @Date: 2021-06-02 17:05:43
 * @Author: chenwei
 */

#include "curvefs/src/mds/spaceclient/space_client.h"

using curvefs::space::InitSpaceRequest;
using curvefs::space::InitSpaceResponse;
using curvefs::space::UnInitSpaceRequest;
using curvefs::space::UnInitSpaceResponse;
using curvefs::space::SpaceStatusCode;
using curvefs::space::SpaceAllocService_Stub;

namespace curvefs {
namespace mds {
bool SpaceClient::Init() {
    if (channel_.Init(options_.spaceAddr.c_str(), nullptr) != 0) {
        LOG(ERROR) << "Init channel to space: " << options_.spaceAddr
                   << " failed!";
        return false;
    }
    inited_ = true;
    return true;
}

void SpaceClient::Uninit() {
    inited_ = false;
}

FSStatusCode SpaceClient::InitSpace(const FsInfo& fsInfo) {
    if (!inited_) {
        LOG(ERROR) << "SpaceClient not Init, init first";
        return FSStatusCode::SPACE_CLIENT_NOT_INITED;
    }

    InitSpaceRequest request;
    InitSpaceResponse response;

    brpc::Controller cntl;
    cntl.set_timeout_ms(options_.rpcTimeoutMs);

    SpaceAllocService_Stub stub(&channel_);
    FsInfo* fsInfoPtr = request.mutable_fsinfo();
    fsInfoPtr->CopyFrom(fsInfo);

    stub.InitSpace(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "InitSpace failed, Rpc error = " << cntl.ErrorText();
        return FSStatusCode::RPC_ERROR;
    }

    if (response.status() != SpaceStatusCode::SPACE_OK) {
        LOG(ERROR) << "InitSpace failed, ret = "
                   << FSStatusCode::INIT_SPACE_ERROR;
        return FSStatusCode::INIT_SPACE_ERROR;
    }
    return FSStatusCode::OK;
}

FSStatusCode SpaceClient::UnInitSpace(uint32_t fsId) {
    if (!inited_) {
        LOG(ERROR) << "SpaceClient not Init, init first";
        return FSStatusCode::SPACE_CLIENT_NOT_INITED;
    }

    UnInitSpaceRequest request;
    UnInitSpaceResponse response;

    brpc::Controller cntl;
    cntl.set_timeout_ms(options_.rpcTimeoutMs);

    SpaceAllocService_Stub stub(&channel_);

    request.set_fsid(fsId);

    stub.UnInitSpace(&cntl, &request, &response, nullptr);

    if (cntl.Failed()) {
        LOG(ERROR) << "UnInitSpace failed, Rpc error = " << cntl.ErrorText();
        return FSStatusCode::RPC_ERROR;
    }

    if (response.status() != SpaceStatusCode::SPACE_OK) {
        LOG(ERROR) << "UnInitSpace failed, ret = "
                   << FSStatusCode::UNINIT_SPACE_ERROR;
        return FSStatusCode::UNINIT_SPACE_ERROR;
    }
    return FSStatusCode::OK;
}
}  // namespace mds
}  // namespace curvefs

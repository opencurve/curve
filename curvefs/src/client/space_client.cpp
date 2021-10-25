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
 * Created Date: Thur Jun 17 2021
 * Author: lixiaocui
 */


#include "curvefs/src/client/space_client.h"

namespace curvefs {
namespace client {

CURVEFS_ERROR
SpaceAllocServerClientImpl::SpaceAllocRPCExcutor::DoRPCTask(RPCFunc task) {
    brpc::Channel channel;
    int ret = channel.Init(opt_.spaceaddr.c_str(), nullptr);
    if (ret != 0) {
        LOG(WARNING) << "Init channel failed, addr = " << opt_.spaceaddr;
        return static_cast<CURVEFS_ERROR>(-EHOSTDOWN);
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(opt_.rpcTimeoutMs);

    return task(&channel, &cntl);
}

CURVEFS_ERROR
SpaceAllocServerClientImpl::Init(const SpaceAllocServerOption &spaceopt,
                                 SpaceBaseClient *baseclient) {
    basecli_ = baseclient;
    excutor_.SetOption(spaceopt);
    return CURVEFS_ERROR::OK;
}

#define RPCTaskDefine                                                          \
    [&](brpc::Channel * channel, brpc::Controller * cntl) -> CURVEFS_ERROR

CURVEFS_ERROR
SpaceAllocServerClientImpl::AllocExtents(
    uint32_t fsId, const std::list<ExtentAllocInfo> &toAllocExtents,
    curvefs::space::AllocateType type, std::list<Extent> *allocatedExtents) {
    auto task = RPCTaskDefine {
        auto iter = toAllocExtents.begin();
        CURVEFS_ERROR retcode = CURVEFS_ERROR::UNKNOWN;
        while (iter != toAllocExtents.end()) {
            AllocateSpaceResponse response;
            basecli_->AllocExtents(fsId, *iter, type, &response, cntl, channel);
            if (cntl->Failed()) {
                LOG(WARNING)
                    << "AllocExtents Failed, errorcode = " << cntl->ErrorCode()
                    << ", error content:" << cntl->ErrorText()
                    << ", log id = " << cntl->log_id();
                return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
            }

            SpaceStatusCode stcode = response.status();
            SpaceStatusCode2CurveFSErr(stcode, &retcode);
            LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
                << "AllocExtents: fsId = " << fsId << ", errcode = " << retcode
                << ", errmsg = " << SpaceStatusCode_Name(stcode);
            if (retcode != CURVEFS_ERROR::OK) {
                return static_cast<CURVEFS_ERROR>(stcode);
            }

            for (int i = 0; i < response.extents_size(); ++i) {
                allocatedExtents->push_back(response.extents(i));
            }
            ++iter;
        }

        return retcode;
    };

    return excutor_.DoRPCTask(task);
}

CURVEFS_ERROR
SpaceAllocServerClientImpl::DeAllocExtents(uint32_t fsId,
                                           std::list<Extent> allocatedExtents) {
    auto task = RPCTaskDefine {
        DeallocateSpaceResponse response;
        basecli_->DeAllocExtents(fsId, allocatedExtents, &response, cntl,
                                 channel);
        if (cntl->Failed()) {
            LOG(WARNING) << "AllocExtents Failed, errorcode = "
                         << cntl->ErrorCode()
                         << ", error content:" << cntl->ErrorText()
                         << ", log id = " << cntl->log_id();
            return static_cast<CURVEFS_ERROR>(-cntl->ErrorCode());
        }

        CURVEFS_ERROR retcode = CURVEFS_ERROR::UNKNOWN;
        SpaceStatusCode stcode = response.status();
        SpaceStatusCode2CurveFSErr(stcode, &retcode);
        LOG_IF(WARNING, retcode != CURVEFS_ERROR::OK)
            << "AllocExtents: fsId = " << fsId << ", errcode = " << retcode
            << ", errmsg = " << SpaceStatusCode_Name(stcode);
        return retcode;
    };
    return excutor_.DoRPCTask(task);
}


void SpaceAllocServerClientImpl::SpaceStatusCode2CurveFSErr(
    const SpaceStatusCode &statcode, CURVEFS_ERROR *errcode) {
    switch (statcode) {
    case SpaceStatusCode::SPACE_OK:
        *errcode = CURVEFS_ERROR::OK;
        break;
    case SpaceStatusCode::SPACE_NO_SPACE:
        *errcode = CURVEFS_ERROR::NO_SPACE;
        break;
    default:
        *errcode = CURVEFS_ERROR::UNKNOWN;
        break;
    }
}


}  // namespace client
}  // namespace curvefs

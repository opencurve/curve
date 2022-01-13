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
 * Date: Wed Sep  1 17:28:51 CST 2021
 * Author: wuhanqing
 */

// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#include "curvefs/src/metaserver/copyset/raft_cli2.h"

#include <string>

#include "curvefs/src/metaserver/copyset/utils.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

butil::Status GetLeader(PoolId poolId, CopysetId copysetId,
                        const braft::Configuration& conf,
                        braft::PeerId* leaderId) {
    if (conf.empty()) {
        return butil::Status(EINVAL, "Empty group configuration");
    }

    butil::Status status(-1, "Fail to get leader of copyset %s",
                         ToGroupIdString(poolId, copysetId).c_str());

    leaderId->reset();
    for (const auto& peer : conf) {
        brpc::Channel channel;
        if (channel.Init(peer.addr, nullptr) != 0) {
            return butil::Status(-1, "Fail to init channel to %s",
                                 peer.to_string().c_str());
        }

        CliService2_Stub stub(&channel);
        GetLeaderRequest2 request;
        GetLeaderResponse2 response;
        brpc::Controller cntl;
        request.set_poolid(poolId);
        request.set_copysetid(copysetId);
        request.mutable_peer()->set_address(peer.to_string());

        stub.GetLeader(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            std::string saved = status.error_str();
            status.set_error(cntl.ErrorCode(), "%s, [%s] %s", saved.c_str(),
                             butil::endpoint2str(cntl.remote_side()).c_str(),
                             cntl.ErrorText().c_str());
            continue;
        } else {
            leaderId->parse(response.leader().address());
            break;
        }
    }

    if (leaderId->is_empty()) {
        return status;
    }

    return butil::Status::OK();
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

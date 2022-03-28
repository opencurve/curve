/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Created Date: 2022-04-29
 * Author: chengyi01
 */

#include "curvefs/src/tools/query/curvefs_inode.h"

#include <iostream>
#include <utility>
#include <vector>

DECLARE_string(metaserverAddr);
DECLARE_string(poolId);
DECLARE_string(copysetId);
DECLARE_string(partitionId);
DECLARE_string(fsId);
DECLARE_string(inodeId);

#include "src/common/string_util.h"

namespace curvefs {
namespace tools {
namespace query {

void InodeTool::PrintHelp() {
    CurvefsToolRpc::PrintHelp();
    std::cout << " -poolId=" << FLAGS_poolId
              << " -copysetId=" << FLAGS_copysetId
              << " -partitionId=" << FLAGS_partitionId
              << " -fsId=" << FLAGS_fsId << " -inodeId=" << FLAGS_inodeId
              << " [-metaserverAddr=" << FLAGS_metaserverAddr
              << " -rpcStreamIdleTimeoutMs=" << FLAGS_rpcStreamIdleTimeoutMs
              << "]";
    std::cout << std::endl;
}

int InodeTool::Init() {
    if (CurvefsToolRpc::Init() != 0) {
        return -1;
    }

    curve::common::SplitString(FLAGS_metaserverAddr, ",", &hostsAddr_);

    std::vector<std::string> poolsId;
    curve::common::SplitString(FLAGS_poolId, ",", &poolsId);

    std::vector<std::string> copysetsId;
    curve::common::SplitString(FLAGS_copysetId, ",", &copysetsId);

    std::vector<std::string> partitionId;
    curve::common::SplitString(FLAGS_partitionId, ",", &partitionId);

    std::vector<std::string> fsId;
    curve::common::SplitString(FLAGS_fsId, ",", &fsId);

    std::vector<std::string> inodeId;
    curve::common::SplitString(FLAGS_inodeId, ",", &inodeId);

    if (poolsId.size() != copysetsId.size() ||
        poolsId.size() != partitionId.size() || poolsId.size() != fsId.size() ||
        poolsId.size() != inodeId.size()) {
        std::cout << "fsId:" << FLAGS_fsId << " poolId:" << FLAGS_poolId
                  << " copysetId:" << FLAGS_copysetId
                  << " partitionId:" << FLAGS_partitionId
                  << " inodeId:" << FLAGS_inodeId << " must be the same size"
                  << std::endl;
        return -1;
    }

    for (size_t i = 0; i < poolsId.size(); ++i) {
        curvefs::metaserver::GetInodeRequest request;
        request.set_poolid(std::stoi(poolsId[i]));
        request.set_copysetid(std::stoi(copysetsId[i]));
        request.set_partitionid(std::stoi(partitionId[i]));
        request.set_fsid(std::stoi(fsId[i]));
        request.set_inodeid(std::stoi(inodeId[i]));
        SetStreamingRpc(false);
        request.set_supportstreaming(true);
        AddRequest(request);
    }

    service_stub_func_ =
        std::bind(&curvefs::metaserver::MetaServerService_Stub::GetInode,
                  service_stub_.get(), std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, nullptr);

    return 0;
}

void InodeTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMetaserverAddr);
    AddUpdateFlagsFunc(curvefs::tools::SetRpcStreamIdleTimeoutMs);
}

bool InodeTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = false;
    if (controller_->Failed()) {
        errorOutput_ << "send request "
                     << " to metaserver: " << host
                     << " failed, errorcode= " << controller_->ErrorCode()
                     << ", error text " << controller_->ErrorText() << "\n";
    } else  {
        if (response_->statuscode() == metaserver::MetaStatusCode::OK) {
            ret = UpdateInode2InodeBaseInfoList_(requestQueue_.front(),
                                                 response_->inode());
            if (show_) {
                std::cout << "inode:\n" << requestQueue_.front().DebugString()
                          << response_->inode().DebugString();
            }
        } else {
            errorOutput_ << "request: "
                         << requestQueue_.front().ShortDebugString()
                         << " get response: " << response_->ShortDebugString()
                         << std::endl;
        }
    }
    return ret;
}

bool InodeTool::CheckRequiredFlagDefault() {
    google::CommandLineFlagInfo info;
    if (CheckPoolIdDefault(&info) && CheckCopysetIdDefault(&info) &&
        CheckPartitionIdDefault(&info) && CheckFsIdDefault(&info) &&
        CheckInodeIdDefault(&info)) {
        std::cerr << "no -poolId=*,* -copysetId=*,* -partitionId=*,* -fsId=*,* "
                     "-inodeId=*,*"
                  << std::endl;
        return true;
    }
    return false;
}

bool InodeTool::UpdateInode2InodeBaseInfoList_(const InodeBase& inode,
                                               const InodeBaseInfo& list) {
    bool ret = true;
    auto iter = std::find_if(
        inode2InodeBaseInfoList_.begin(), inode2InodeBaseInfoList_.end(),
        [inode](const std::pair<InodeBase, std::vector<InodeBaseInfo>>& a) {
            return a.first.fsid() == inode.fsid() &&
                   a.first.inodeid() == inode.inodeid();
        });
    if (iter == inode2InodeBaseInfoList_.end()) {
        inode2InodeBaseInfoList_.insert(
            {inode, std::vector<InodeBaseInfo>{list}});
    } else {
        iter->second.emplace_back(list);
        ret = false;
    }
    return ret;
}

}  // namespace query
}  // namespace tools
}  // namespace curvefs

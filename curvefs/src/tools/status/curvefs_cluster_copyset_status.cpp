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
 * Created Date: 2021-10-31
 * Author: chengyi01
 */
#include "curvefs/src/tools/status/curvefs_cluster_copyset_status.h"

DECLARE_string(mdsAddr);
DECLARE_bool(detail);
DECLARE_string(metaserverAddr);

namespace curvefs {
namespace tools {
namespace status {

void ClusterCopysetStatusTool::PrintHelp() {
    CurvefsTool::PrintHelp();
    std::cout << " [-mdsAddr=" << FLAGS_mdsAddr << "]"
              << " [-metaserverAddr=" << FLAGS_mdsAddr << "]"
              << " [-detail=" << FLAGS_detail << "]";
    std::cout << std::endl;
}

int ClusterCopysetStatusTool::Init() {
    int ret = 0;
    getFsId_ = std::make_shared<query::FsInfoListTool>(false);
    getFsId_->Init();
    getPartition_ = std::make_shared<query::PartitionListTool>(false);
    getPartition_->Init();
    getCopysetinfo_ = std::make_shared<query::CopysetQueryTool>(false);
    getCopysetinfo_->Init();
    getCopysetStatus_ = std::make_shared<status::CopysetStatusTool>(false);
    getCopysetStatus_->Init();
    return ret;
}

int ClusterCopysetStatusTool::RunCommand() {
    int ret = 0;

    // get fsId
    getFsId_->Run();
    auto fsinfoVec = getFsId_->GetResponse()->fsinfo();

    // get partitionlist(contain copysetid)
    curvefs::mds::topology::ListPartitionsRequest listPartitionRequest;
    for (auto const& i : fsinfoVec) {
        listPartitionRequest.add_fsid(i.fsid());
    }
    std::queue<decltype(listPartitionRequest)> queueListPartition;
    queueListPartition.push(listPartitionRequest);
    getPartition_->SetRequestQueue(queueListPartition);
    getPartition_->RunCommand();
    auto fsid2partition = getPartition_->GetResponse()->fsid2partitionlist();

    // get copysetInfo
    std::set<uint32_t> copysetIdSet;  // unique copysetId
    for (auto const& i : fsid2partition) {
        for (auto const& j : i.second.partitioninfolist()) {
            copysetIdSet.insert(j.copysetid());
        }
    }
    curvefs::mds::topology::GetCopysetInfoRequest listcopysetInfoRequest;
    for (auto const i : copysetIdSet) {
        listcopysetInfoRequest.add_copysetid(i);
    }
    std::queue<decltype(listcopysetInfoRequest)> queueListCopysetInfo;
    queueListCopysetInfo.push(listcopysetInfoRequest);
    getCopysetinfo_->SetRequestQueue(queueListCopysetInfo);
    getCopysetinfo_->RunCommand();
    auto copysetInfoLsit = getCopysetinfo_->GetResponse()->copysetinfo();

    // get copysetSatus
    curvefs::metaserver::copyset::GetCopysetsStatusRequest listCopysetStatus;
    for (auto const& i : copysetInfoLsit) {
        curvefs::metaserver::copyset::CopysetStatusRequest copysetTmp;
        copysetTmp.set_poolid(i.poolid());
        copysetTmp.set_copysetid(i.copysetid());
        copysetTmp.set_allocated_peer(
            new curvefs::common::Peer(i.leaderpeer()));
        *listCopysetStatus.add_copysetsinfo() = std::move(copysetTmp);
    }
    std::queue<decltype(listCopysetStatus)> queueListCopysetSatus;
    queueListCopysetSatus.push(listCopysetStatus);
    getCopysetStatus_->SetRequestQueue(queueListCopysetSatus);
    getCopysetStatus_->RunCommand();
    auto copysetStatus = getCopysetStatus_->GetResponse()->copysetsstatus();
    for (auto const& i : copysetStatus) {
        std::cout << i.DebugString() << std::endl;
    }
    return ret;
}

}  // namespace status
}  // namespace tools
}  // namespace curvefs

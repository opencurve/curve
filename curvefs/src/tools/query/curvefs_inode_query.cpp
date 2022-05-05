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
 * Created Date: 2022-03-25
 * Author: chengyi01
 */

#include "curvefs/src/tools/query/curvefs_inode_query.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>

#include "src/common/string_util.h"

DECLARE_string(mdsAddr);
DECLARE_string(metaserverAddr);
DECLARE_string(poolId);
DECLARE_string(copysetId);
DECLARE_string(partitionId);
DECLARE_string(fsId);
DECLARE_string(inodeId);
DECLARE_uint32(rpcStreamIdleTimeoutMs);

namespace curvefs {
namespace tools {
namespace query {

void InodeQueryTool::PrintHelp() {
    CurvefsTool::PrintHelp();
    std::cout << " -fsId=" << FLAGS_fsId << " -inodeId=" << FLAGS_inodeId
              << " [-mds=" << FLAGS_mdsAddr
              << " -metaserverAddr=" << FLAGS_metaserverAddr
              << " -rpcStreamIdleTimeoutMs=" << FLAGS_rpcStreamIdleTimeoutMs
              << "]";
    std::cout << std::endl;
}

int InodeQueryTool::Init() {
    std::vector<std::string> fsIds;
    curve::common::SplitString(FLAGS_fsId, ",", &fsIds);
    std::vector<std::string> inodeIds;
    curve::common::SplitString(FLAGS_inodeId, ",", &inodeIds);
    if (fsIds.size() != inodeIds.size()) {
        std::cout << "fsId and inodeId must be the same size" << std::endl;
        return -1;
    }
    for (size_t i = 0; i < fsIds.size(); ++i) {
        InodeBase tmp;
        uint64_t tmpFsId = 0;
        curve::common::StringToUll(fsIds[i], &tmpFsId);
        tmp.set_fsid(tmpFsId);

        uint64_t tmpInodeId = 0;
        curve::common::StringToUll(inodeIds[i], &tmpInodeId);
        tmp.set_inodeid(tmpInodeId);

        inodeBases_.emplace_back(tmp);
    }
    return 0;
}

int InodeQueryTool::RunCommand() {
    // get fs partition list
    int ret = partitionListTool_.Run();
    fsId2PartitionList_ = partitionListTool_.GetFsId2PartitionInfoList();

    // clear and reset flag
    FLAGS_fsId = FLAGS_poolId = FLAGS_copysetId = FLAGS_partitionId =
        FLAGS_inodeId = "";
    for (auto& inode : inodeBases_) {
        if (GetInodeInfo(&inode)) {
            FLAGS_fsId.append(std::to_string(inode.fsid()) + ",");
            FLAGS_poolId.append(std::to_string(inode.poolid()) + ",");
            FLAGS_copysetId.append(std::to_string(inode.copysetid()) + ",");
            FLAGS_partitionId.append(std::to_string(inode.partitionid()) + ",");
            FLAGS_inodeId.append(std::to_string(inode.inodeid()) + ",");
        }
    }

    // get InodeBaseInfo
    if (inodeTool_.Run() != 0) {
        ret = -1;
    }
    inode2InodeBaseInfoList_ = inodeTool_.GetInode2InodeBaseInfoList();

    // get s3chunkinfo
    if (inodeS3InfoMapTool_.Run() != 0) {
        ret = -1;
    }
    inode2S3ChunkInfoList_ = inodeS3InfoMapTool_.GetInode2S3ChunkInfoList();

    // print
    for (auto const& i : inodeBases_) {
        std::cout << "fsId: " << i.fsid()
                  << " inodeId: " << i.inodeid() << std::endl;
        // base info
        auto iter1 = inode2InodeBaseInfoList_.find(i);
        if (iter1 != inode2InodeBaseInfoList_.end()) {
            if (iter1->second.empty()) {
                std::cerr << "inode base info is empty" << std::endl;
                ret = -1;
            }
            for (auto const& base : iter1->second) {
                std::cout << "-baseInfo:\n" << base.DebugString();
            }
        } else {
            std::cerr << "inode baseinfo not found" << std::endl;
            ret = -1;
        }

        // s3 chunk info map
        auto iter2 = inode2S3ChunkInfoList_.find(i);
        if (iter2 != inode2S3ChunkInfoList_.end()) {
            if (iter2->second.s3chunks().empty()) {
                std::cout << "no s3chunkinfo" << std::endl;
            }
            for (auto const& s3Chunkinfo : iter2->second.s3chunks()) {
                std::cout << "-----------------\n-s3ChunkInfo:\n"
                          << s3Chunkinfo.DebugString() << std::endl;
            }
            std::cout << "Total s3chunkinfo: "
                      << iter2->second.s3chunks().size() << std::endl;
        } else {
            std::cerr << "inode s3ChunkinfoMap not found" << std::endl;
            ret = -1;
        }

        std::cout << std::endl;
    }

    return ret;
}

bool InodeQueryTool::GetInodeInfo(InodeBase* inode) {
    // find fs partitionlist
    auto fs2ParListIter = fsId2PartitionList_.find(inode->fsid());
    if (fs2ParListIter == fsId2PartitionList_.end()) {
        errorOutput_ << "fsId: " << inode->fsid() << " no partition!";
        return false;
    }

    // find partition which inode belongs to
    auto partitionIter = std::find_if(
        fs2ParListIter->second.begin(), fs2ParListIter->second.end(),
        [=](const common::PartitionInfo& a) -> bool {
            return a.fsid() == inode->fsid() && a.start() <= inode->inodeid() &&
                   a.end() >= inode->inodeid();
        });
    if (partitionIter == fs2ParListIter->second.end()) {
        errorOutput_ << "fsId: " << inode->fsid()
                     << " inodeId: " << inode->inodeid()
                     << " not found in any partition!";
        return false;
    }

    // set inode
    inode->set_poolid(partitionIter->poolid());
    inode->set_partitionid(partitionIter->partitionid());
    inode->set_copysetid(partitionIter->copysetid());
    return true;
}

}  // namespace query
}  // namespace tools
}  // namespace curvefs

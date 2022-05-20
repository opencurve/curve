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

#include "curvefs/src/tools/query/curvefs_inode_s3infomap.h"

#include <algorithm>
#include <iostream>
#include <memory>

#include "src/common/string_util.h"

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

InodeBase TraslateInodeBase(
    const curvefs::metaserver::GetOrModifyS3ChunkInfoRequest& source) {
    InodeBase target;
    target.set_poolid(source.poolid());
    target.set_copysetid(source.copysetid());
    target.set_partitionid(source.partitionid());
    target.set_fsid(source.fsid());
    target.set_inodeid(source.inodeid());
    return target;
}

void InodeS3InfoMapTool::PrintHelp() {
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

void InodeS3InfoMapTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMetaserverAddr);
    AddUpdateFlagsFunc(curvefs::tools::SetRpcStreamIdleTimeoutMs);
}

int InodeS3InfoMapTool::Init() {
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
        curvefs::metaserver::GetOrModifyS3ChunkInfoRequest request;
        request.set_poolid(std:: stoul((poolsId[i])));
        request.set_copysetid(std:: stoul((copysetsId[i])));
        request.set_partitionid(std:: stoul((partitionId[i])));
        request.set_fsid(std:: stoul((fsId[i])));
        request.set_inodeid(std:: stoull((inodeId[i])));
        request.set_returns3chunkinfomap(true);
        SetStreamingRpc(true);
        request.set_supportstreaming(isStreaming_);
        AddRequest(request);
    }

    service_stub_func_ = std::bind(
        &curvefs::metaserver::MetaServerService_Stub::GetOrModifyS3ChunkInfo,
        service_stub_.get(), std::placeholders::_1, std::placeholders::_2,
        std::placeholders::_3, nullptr);
    SetReceiveCallback();
    return 0;
}

bool InodeS3InfoMapTool::AfterSendRequestToHost(const std::string& host) {
    bool ret = false;
    if (controller_->Failed()) {
        errorOutput_ << "send request "
                     << " to metaserver: " << host
                     << " failed, errorcode= " << controller_->ErrorCode()
                     << ", error text " << controller_->ErrorText() << "\n";
    } else {
        if (response_->statuscode() == metaserver::MetaStatusCode::OK) {
            if (!isStreaming_) {
                if (response_->s3chunkinfomap_size() == 0) {
                    UpdateInode2S3ChunkInfoList_(
                        TraslateInodeBase(requestQueue_.front()),
                        S3ChunkInfoList());
                }
                for (auto const& list : response_->s3chunkinfomap()) {
                    UpdateInode2S3ChunkInfoList_(
                        TraslateInodeBase(requestQueue_.front()), list.second);
                }
            }
            if (show_) {
                if (isStreaming_) {
                    for (auto const& i : inode2S3ChunkInfoList_) {
                        std::cout << "fsId: " << i.first.fsid()
                                  << " inodeId: " << i.first.inodeid()
                                  << std::endl;
                        for (auto const& j : i.second.s3chunks()) {
                            std::cout << "  s3ChunkInfo: " << j.DebugString()
                                      << std::endl;
                        }
                    }
                } else {
                    std::cout << "response: " << response_->DebugString()
                              << std::endl;
                }
                ret = true;
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

bool InodeS3InfoMapTool::CheckRequiredFlagDefault() {
    google::CommandLineFlagInfo info;
    if (CheckPoolIdDefault(&info) && CheckCopysetIdDefault(&info) &&
        CheckPartitionIdDefault(&info) && CheckFsIdDefault(&info) &&
        CheckInodeIdDefault(&info)) {
        std::cerr << "no -poolId=*,* -copysetId=*,* -partitionId=*,* -fsId=*,* "
                     "-inodeId=*,* , please use -example!"
                  << std::endl;
        return true;
    }
    return false;
}

void InodeS3InfoMapTool::SetReceiveCallback() {
    receiveCallback_ = [&](butil::IOBuf* buffer) -> bool {
        uint64_t chunkIndex;
        InodeBase inode = TraslateInodeBase(requestQueue_.front());
        S3ChunkInfoList list;
        butil::IOBuf out;
        std::string delim = ":";
        // parse s3 meta stream buffer
        if (buffer->cut_until(&out, delim) != 0) {
            std::cerr << "invalid stream buffer: no delimiter";
            return false;
        }
        if (!curve::common::StringToUll(out.to_string(), &chunkIndex)) {
            std::cerr << "invalid stream buffer: invalid chunkIndex";
            return false;
        }
        if (!brpc::ParsePbFromIOBuf(&list, *buffer)) {
            std::cerr << "invalid stream buffer: invalid s3chunkinfo list";
            return false;
        }

         // handle  meta stream buffer
        UpdateInode2S3ChunkInfoList_(inode, list);
        return true;
    };
}

void InodeS3InfoMapTool::UpdateInode2S3ChunkInfoList_(const InodeBase& inode,
                                            const S3ChunkInfoList& list) {
    auto merge = [](const S3ChunkInfoList& source, S3ChunkInfoList* target) {
        for (int i = 0; i < source.s3chunks_size(); i++) {
            auto* chunkinfo = target->add_s3chunks();
            *chunkinfo = source.s3chunks(i);
        }
    };

    auto iter = std::find_if(
        inode2S3ChunkInfoList_.begin(), inode2S3ChunkInfoList_.end(),
        [inode](const std::pair<InodeBase, S3ChunkInfoList>& a) {
            return a.first.fsid() == inode.fsid() &&
                   a.first.inodeid() == inode.inodeid();
        });
    if (iter == inode2S3ChunkInfoList_.end()) {
        inode2S3ChunkInfoList_.insert({inode, list});
    } else {
        merge(list, &iter->second);
    }
}

}  // namespace query
}  // namespace tools
}  // namespace curvefs

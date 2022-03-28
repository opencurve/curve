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

#ifndef CURVEFS_SRC_TOOLS_QUERY_CURVEFS_INODE_S3INFOMAP_H_
#define CURVEFS_SRC_TOOLS_QUERY_CURVEFS_INODE_S3INFOMAP_H_

#include <brpc/channel.h>
#include <gflags/gflags.h>

#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/tools/curvefs_tool.h"
#include "curvefs/src/tools/curvefs_tool_define.h"

namespace curvefs {
namespace tools {
namespace query {

using curvefs::metaserver::S3ChunkInfoList;

using HostAndResponseType =
    std::vector<std::pair<std::string,
                          curvefs::metaserver::GetOrModifyS3ChunkInfoResponse>>;

using InodeBase = curvefs::metaserver::GetInodeRequest;

struct HashInodeBase {
    size_t operator()(const InodeBase& inode) const {
        auto inodeIdHash = std::hash<uint64_t>()(inode.inodeid());
        auto fsIdHash = std::hash<uint64_t>()(inode.fsid());
        return inodeIdHash ^ fsIdHash;
    }
};

struct KeyEuqalInodeBase {
    bool operator()(const InodeBase& a, const InodeBase& b) const {
        return a.fsid() == b.fsid() && a.inodeid() == b.inodeid();
    }
};

class InodeS3InfoMapTool
    : public CurvefsToolRpc<curvefs::metaserver::GetOrModifyS3ChunkInfoRequest,
                            curvefs::metaserver::GetOrModifyS3ChunkInfoResponse,
                            curvefs::metaserver::MetaServerService_Stub> {
 public:
    explicit InodeS3InfoMapTool(const std::string& cmd = kNoInvokeCmd,
                                bool show = true)
        : CurvefsToolRpc(cmd) {
        show_ = show;
    }
    void PrintHelp() override;
    int Init() override;
    std::unordered_map<InodeBase, S3ChunkInfoList, HashInodeBase,
                       KeyEuqalInodeBase>
    GetInode2S3ChunkInfoList() {
        return inode2S3ChunkInfoList_;
    }

 protected:
    void AddUpdateFlags() override;
    bool AfterSendRequestToHost(const std::string& host) override;
    bool CheckRequiredFlagDefault() override;
    void SetReceiveCallback();
    void UpdateInode2S3ChunkInfoList_(const InodeBase& inode,
                                      const S3ChunkInfoList&list);

 protected:
    std::unordered_map<InodeBase, S3ChunkInfoList, HashInodeBase,
                       KeyEuqalInodeBase>
        inode2S3ChunkInfoList_;
};

}  // namespace query
}  // namespace tools
}  // namespace curvefs

#endif  // CURVEFS_SRC_TOOLS_QUERY_CURVEFS_INODE_S3INFOMAP_H_

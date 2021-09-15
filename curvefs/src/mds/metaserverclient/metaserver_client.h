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
 * @Date: 2021-06-24 11:21:45
 * @Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_METASERVERCLIENT_METASERVER_CLIENT_H_
#define CURVEFS_SRC_MDS_METASERVERCLIENT_METASERVER_CLIENT_H_

#include <brpc/channel.h>
#include <string>
#include <set>
#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/copyset.pb.h"
#include "curvefs/proto/cli2.pb.h"

using curvefs::metaserver::FsFileType;
using curvefs::metaserver::copyset::GetLeaderRequest2;
using curvefs::metaserver::copyset::GetLeaderResponse2;
using curvefs::metaserver::copyset::CliService2_Stub;
using curvefs::metaserver::copyset::CreateCopysetRequest;
using curvefs::metaserver::copyset::CreateCopysetResponse;
using curvefs::common::PartitionInfo;

namespace curvefs {
namespace mds {
struct MetaserverOptions {
    std::string metaserverAddr;
    uint32_t rpcTimeoutMs;
};

class MetaserverClient {
 public:
    explicit MetaserverClient(const MetaserverOptions &option) {
        options_ = option;
    }

    bool Init();

    void Uninit();

    virtual ~MetaserverClient() {}

    virtual FSStatusCode DeleteInode(uint32_t fsId, uint64_t inodeId);

    virtual FSStatusCode CreateRootInode(uint32_t fsId, uint32_t poolId,
                        uint32_t copysetId, uint32_t partitionId, uint32_t uid,
                        uint32_t gid, uint32_t mode, const std::string &leader);

    virtual  FSStatusCode GetLeader(uint32_t poolId, uint32_t copysetId,
                           const std::set<std::string> &addrs,
                           std::string *leader);

    virtual FSStatusCode CreatePartition(uint32_t fsId, uint32_t poolId,
                                 uint32_t copysetId, uint32_t partitionId,
                                 uint64_t idStart, uint64_t idEnd,
                                 const std::string &addr);

    virtual FSStatusCode CreateCopySet(uint32_t poolId,
                                std::set<uint32_t> copysetIds,
                                const std::set<std::string> &addrs);

 private:
    MetaserverOptions options_;
    bool inited_;
    brpc::Channel channel_;
};
}  // namespace mds
}  // namespace curvefs
#endif  // CURVEFS_SRC_MDS_METASERVERCLIENT_METASERVER_CLIENT_H_

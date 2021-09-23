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
 * Created Date: Fri Jun 11 2021
 * Author: lixiaocui
 */
#ifndef CURVEFS_SRC_CLIENT_RPCCLIENT_BASE_CLIENT_H_
#define CURVEFS_SRC_CLIENT_RPCCLIENT_BASE_CLIENT_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <list>
#include <string>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/proto/space.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/client/common/extent.h"
#include "src/client/client_common.h"

namespace curvefs {
namespace client {
namespace rpcclient {
using curvefs::metaserver::CreateDentryRequest;
using curvefs::metaserver::CreateDentryResponse;
using curvefs::metaserver::CreateInodeRequest;
using curvefs::metaserver::CreateInodeResponse;
using curvefs::metaserver::DeleteDentryRequest;
using curvefs::metaserver::DeleteDentryResponse;
using curvefs::metaserver::PrepareRenameTxRequest;
using curvefs::metaserver::PrepareRenameTxResponse;
using curvefs::metaserver::DeleteInodeRequest;
using curvefs::metaserver::DeleteInodeResponse;
using curvefs::metaserver::Dentry;
using ::curvefs::metaserver::FsFileType;
using curvefs::metaserver::GetDentryRequest;
using curvefs::metaserver::GetDentryResponse;
using curvefs::metaserver::GetInodeRequest;
using curvefs::metaserver::GetInodeResponse;
using curvefs::metaserver::Inode;
using curvefs::metaserver::ListDentryRequest;
using curvefs::metaserver::ListDentryResponse;
using curvefs::metaserver::UpdateInodeRequest;
using curvefs::metaserver::UpdateInodeResponse;

using curvefs::common::FSType;
using curvefs::common::S3Info;
using curvefs::common::Volume;
using curvefs::common::PartitionInfo;
using curvefs::common::PartitionStatus;
using curvefs::common::Peer;

using curvefs::mds::CreateFsRequest;
using curvefs::mds::CreateFsResponse;
using curvefs::mds::DeleteFsRequest;
using curvefs::mds::DeleteFsResponse;
using curvefs::mds::FsInfo;
using curvefs::mds::FsStatus;
using curvefs::mds::GetFsInfoRequest;
using curvefs::mds::GetFsInfoResponse;
using curvefs::mds::MountFsRequest;
using curvefs::mds::MountFsResponse;
using curvefs::mds::UmountFsRequest;
using curvefs::mds::UmountFsResponse;

using curvefs::space::AllocateSpaceRequest;
using curvefs::space::AllocateSpaceResponse;
using curvefs::space::DeallocateSpaceRequest;
using curvefs::space::DeallocateSpaceResponse;
using curvefs::space::Extent;

using ::curve::client::CopysetID;
using ::curve::client::LogicPoolID;

using curvefs::mds::topology::PartitionTxId;
using curvefs::mds::topology::CommitTxRequest;
using curvefs::mds::topology::CommitTxResponse;
using curvefs::mds::topology::GetMetaServerInfoRequest;
using curvefs::mds::topology::GetMetaServerInfoResponse;
using curvefs::mds::topology::GetMetaServerListInCopySetsRequest;
using curvefs::mds::topology::GetMetaServerListInCopySetsResponse;
using curvefs::mds::topology::CreatePartitionRequest;
using curvefs::mds::topology::CreatePartitionResponse;
using curvefs::mds::topology::ListPartitionRequest;
using curvefs::mds::topology::ListPartitionResponse;
using curvefs::mds::topology::GetCopysetOfPartitionRequest;
using curvefs::mds::topology::GetCopysetOfPartitionResponse;
using curvefs::mds::topology::TopoStatusCode;
using curvefs::mds::topology::Copyset;
struct InodeParam {
    uint64_t fsId;
    uint64_t length;
    uint32_t uid;
    uint32_t gid;
    uint32_t mode;
    FsFileType type;
    std::string symlink;
};

inline std::ostream& operator<<(std::ostream& os, const InodeParam& p) {
    os << "fsid: " << p.fsId << ", length: " << p.length << ", uid: " << p.uid
       << ", gid: " << p.gid << ", mode: " << p.mode << ", type: " << p.type
       << ", symlink: " << p.symlink;

    return os;
}

class MDSBaseClient {
 public:
    virtual void CreateFs(const std::string &fsName, uint64_t blockSize,
                          const Volume &volume, CreateFsResponse *response,
                          brpc::Controller *cntl, brpc::Channel *channel);

    virtual void CreateFsS3(const std::string &fsName, uint64_t blockSize,
                            const S3Info &s3Info, CreateFsResponse *response,
                            brpc::Controller *cntl, brpc::Channel *channel);

    virtual void DeleteFs(const std::string &fsName, DeleteFsResponse *response,
                          brpc::Controller *cntl, brpc::Channel *channel);

    virtual void MountFs(const std::string &fsName, const std::string &mountPt,
                         MountFsResponse *response, brpc::Controller *cntl,
                         brpc::Channel *channel);

    virtual void UmountFs(const std::string &fsName, const std::string &mountPt,
                          UmountFsResponse *response, brpc::Controller *cntl,
                          brpc::Channel *channel);

    virtual void GetFsInfo(const std::string &fsName,
                           GetFsInfoResponse *response, brpc::Controller *cntl,
                           brpc::Channel *channel);

    virtual void GetFsInfo(uint32_t fsId, GetFsInfoResponse *response,
                           brpc::Controller *cntl, brpc::Channel *channel);

    virtual void CommitTx(const std::vector<PartitionTxId>& txIds,
                          CommitTxResponse* response,
                          brpc::Controller* cntl,
                          brpc::Channel* channel);

    virtual void GetMetaServerInfo(uint32_t port, std::string ip,
                                   GetMetaServerInfoResponse *response,
                                   brpc::Controller *cntl,
                                   brpc::Channel *channel);
    virtual void GetMetaServerListInCopysets(
        const LogicPoolID &logicalpooid,
        const std::vector<CopysetID> &copysetidvec,
        GetMetaServerListInCopySetsResponse *response, brpc::Controller *cntl,
        brpc::Channel *channel);

    virtual void CreatePartition(uint32_t fsID, uint32_t count,
                                 CreatePartitionResponse *response,
                                 brpc::Controller *cntl,
                                 brpc::Channel *channel);

    virtual void GetCopysetOfPartitions(
        const std::vector<uint32_t> &partitionIDList,
        GetCopysetOfPartitionResponse *response, brpc::Controller *cntl,
        brpc::Channel *channel);

    virtual void ListPartition(uint32_t fsID, ListPartitionResponse *response,
                               brpc::Controller *cntl, brpc::Channel *channel);
};

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_BASE_CLIENT_H_

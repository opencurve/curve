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
 * Created Date: Mon Sept 1 2021
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_CLIENT_RPCCLIENT_MDS_CLIENT_H_
#define CURVEFS_SRC_CLIENT_RPCCLIENT_MDS_CLIENT_H_
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/rpcclient/base_client.h"
#include "src/client/mds_client.h"
#include "src/client/metacache_struct.h"
#include "curvefs/src/client/metric/client_metric.h"
#include "curvefs/proto/space.pb.h"

using ::curve::client::CopysetID;
using ::curve::client::CopysetInfo;
using ::curve::client::CopysetPeerInfo;
using ::curve::client::LogicPoolID;
using ::curve::client::MDSClient;
using ::curve::client::PeerAddr;
using ::curvefs::client::common::MetaserverID;
using ::curvefs::client::metric::MDSClientMetric;
using ::curvefs::common::PartitionInfo;
using ::curvefs::common::Peer;
using ::curvefs::common::Volume;
using ::curvefs::mds::FsInfo;
using ::curvefs::mds::FSStatusCode;
using ::curvefs::mds::space::SpaceErrCode;
using ::curvefs::mds::topology::Copyset;
using ::curvefs::mds::topology::TopoStatusCode;

namespace curvefs {
namespace client {
namespace rpcclient {

using curvefs::mds::GetLatestTxIdRequest;
using curvefs::mds::GetLatestTxIdResponse;
using curvefs::mds::CommitTxRequest;
using curvefs::mds::CommitTxResponse;
class MdsClient {
 public:
    MdsClient() {}
    virtual ~MdsClient() {}

    virtual FSStatusCode Init(const ::curve::client::MetaServerOption &mdsOpt,
                              MDSBaseClient *baseclient) = 0;

    virtual FSStatusCode MountFs(const std::string &fsName,
                                 const std::string &mountPt,
                                 FsInfo *fsInfo) = 0;

    virtual FSStatusCode UmountFs(const std::string &fsName,
                                  const std::string &mountPt) = 0;

    virtual FSStatusCode GetFsInfo(const std::string &fsName,
                                   FsInfo *fsInfo) = 0;

    virtual FSStatusCode GetFsInfo(uint32_t fsId, FsInfo *fsInfo) = 0;

    virtual bool
    GetMetaServerInfo(const PeerAddr &addr,
                      CopysetPeerInfo<MetaserverID> *metaserverInfo) = 0;

    virtual bool GetMetaServerListInCopysets(
        const LogicPoolID &logicalpooid,
        const std::vector<CopysetID> &copysetidvec,
        std::vector<CopysetInfo<MetaserverID>> *cpinfoVec) = 0;

    virtual bool
    CreatePartition(uint32_t fsid, uint32_t count,
                    std::vector<PartitionInfo> *partitionInfos) = 0;

    virtual bool
    GetCopysetOfPartitions(const std::vector<uint32_t> &partitionIDList,
                           std::map<uint32_t, Copyset> *copysetMap) = 0;

    virtual bool ListPartition(uint32_t fsID,
                               std::vector<PartitionInfo> *partitionInfos) = 0;
    virtual FSStatusCode AllocS3ChunkId(uint32_t fsId, uint64_t *chunkId) = 0;

    virtual FSStatusCode
    RefreshSession(const std::vector<PartitionTxId> &txIds,
                   std::vector<PartitionTxId> *latestTxIdList) = 0;

    virtual FSStatusCode GetLatestTxId(std::vector<PartitionTxId>* txIds) = 0;

    virtual FSStatusCode
    GetLatestTxIdWithLock(uint32_t fsId,
                          const std::string& fsName,
                          const std::string& uuid,
                          std::vector<PartitionTxId>* txIds,
                          uint64_t* sequence) = 0;

    virtual FSStatusCode
    CommitTx(const std::vector<PartitionTxId>& txIds) = 0;

    virtual FSStatusCode
    CommitTxWithLock(const std::vector<PartitionTxId>& txIds,
                     const std::string& fsName,
                     const std::string& uuid,
                     uint64_t sequence) = 0;

    // allocate block group
    virtual SpaceErrCode AllocateVolumeBlockGroup(
        uint32_t fsId,
        uint32_t count,
        const std::string &owner,
        std::vector<curvefs::mds::space::BlockGroup> *groups) = 0;

    // acquire block group
    virtual SpaceErrCode AcquireVolumeBlockGroup(
        uint32_t fsId,
        uint64_t blockGroupOffset,
        const std::string &owner,
        curvefs::mds::space::BlockGroup *groups) = 0;

    // release block group
    virtual SpaceErrCode ReleaseVolumeBlockGroup(
        uint32_t fsId,
        const std::string &owner,
        const std::vector<curvefs::mds::space::BlockGroup> &blockGroups) = 0;
};

class MdsClientImpl : public MdsClient {
 public:
    explicit MdsClientImpl(const std::string &metricPrefix = "")
        : mdsClientMetric_(metricPrefix) {}

    FSStatusCode Init(const ::curve::client::MetaServerOption &mdsOpt,
                      MDSBaseClient *baseclient) override;

    FSStatusCode MountFs(const std::string &fsName, const std::string &mountPt,
                         FsInfo *fsInfo) override;

    FSStatusCode UmountFs(const std::string &fsName,
                          const std::string &mountPt) override;

    FSStatusCode GetFsInfo(const std::string &fsName, FsInfo *fsInfo) override;

    FSStatusCode GetFsInfo(uint32_t fsId, FsInfo *fsInfo) override;

    bool
    GetMetaServerInfo(const PeerAddr &addr,
                      CopysetPeerInfo<MetaserverID> *metaserverInfo) override;

    bool GetMetaServerListInCopysets(
        const LogicPoolID &logicalpooid,
        const std::vector<CopysetID> &copysetidvec,
        std::vector<CopysetInfo<MetaserverID>> *cpinfoVec) override;

    bool CreatePartition(uint32_t fsID, uint32_t count,
                         std::vector<PartitionInfo> *partitionInfos) override;

    bool
    GetCopysetOfPartitions(const std::vector<uint32_t> &partitionIDList,
                           std::map<uint32_t, Copyset> *copysetMap) override;

    bool ListPartition(uint32_t fsID,
                       std::vector<PartitionInfo> *partitionInfos) override;

    FSStatusCode AllocS3ChunkId(uint32_t fsId, uint64_t *chunkId) override;

    FSStatusCode
    RefreshSession(const std::vector<PartitionTxId> &txIds,
                   std::vector<PartitionTxId> *latestTxIdList) override;

    FSStatusCode GetLatestTxId(std::vector<PartitionTxId>* txIds) override;

    FSStatusCode
    GetLatestTxIdWithLock(uint32_t fsId,
                          const std::string& fsName,
                          const std::string& uuid,
                          std::vector<PartitionTxId>* txIds,
                          uint64_t* sequence) override;

    FSStatusCode CommitTx(const std::vector<PartitionTxId>& txIds) override;

    FSStatusCode
    CommitTxWithLock(const std::vector<PartitionTxId>& txIds,
                     const std::string& fsName,
                     const std::string& uuid,
                     uint64_t sequence) override;

    // allocate block group
    SpaceErrCode AllocateVolumeBlockGroup(
        uint32_t fsId,
        uint32_t size,
        const std::string &owner,
        std::vector<curvefs::mds::space::BlockGroup> *groups) override;

    // acquire block group
    SpaceErrCode AcquireVolumeBlockGroup(
        uint32_t fsId,
        uint64_t blockGroupOffset,
        const std::string &owner,
        curvefs::mds::space::BlockGroup *groups) override;

    // release block group
    SpaceErrCode ReleaseVolumeBlockGroup(
        uint32_t fsId,
        const std::string &owner,
        const std::vector<curvefs::mds::space::BlockGroup> &blockGroups)
        override;

 private:
    FSStatusCode ReturnError(int retcode);

    FSStatusCode GetLatestTxId(const GetLatestTxIdRequest& request,
                               GetLatestTxIdResponse* response);

    FSStatusCode CommitTx(const CommitTxRequest& request);

 private:
    MDSBaseClient *mdsbasecli_;
    ::curve::client::RPCExcutorRetryPolicy rpcexcutor_;
    ::curve::client::MetaServerOption mdsOpt_;

    MDSClientMetric mdsClientMetric_;
};

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_MDS_CLIENT_H_

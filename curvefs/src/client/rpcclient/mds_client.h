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
#include <memory>
#include <string>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/client/rpcclient/base_client.h"
#include "curvefs/src/client/common/config.h"
#include "src/client/mds_client.h"
#include "src/client/metacache_struct.h"
#include "curvefs/src/client/common/common.h"

using ::curve::client::CopysetID;
using ::curve::client::CopysetInfo;
using ::curve::client::CopysetPeerInfo;
using ::curve::client::LogicPoolID;
using ::curve::client::MDSClient;
using ::curve::client::PeerAddr;
using ::curvefs::client::common::MetaserverID;
using ::curvefs::common::Volume;
using ::curvefs::mds::FsInfo;
using ::curvefs::mds::FSStatusCode;

namespace curvefs {
namespace client {
namespace rpcclient {

class MdsClient {
 public:
    MdsClient() {}
    virtual ~MdsClient() {}

    virtual FSStatusCode Init(const ::curve::client::MetaServerOption &mdsOpt,
                      MDSBaseClient *baseclient) = 0;

    virtual FSStatusCode CreateFs(const std::string &fsName, uint64_t blockSize,
                                  const Volume &volume) = 0;

    virtual FSStatusCode CreateFsS3(const std::string &fsName,
                                    uint64_t blockSize,
                                    const S3Info &s3Info) = 0;

    virtual FSStatusCode DeleteFs(const std::string &fsName) = 0;

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
};

class MdsClientImpl : public MdsClient {
 public:
    MdsClientImpl() {}

    FSStatusCode Init(const ::curve::client::MetaServerOption &mdsOpt,
                      MDSBaseClient *baseclient);

    FSStatusCode CreateFs(const std::string &fsName, uint64_t blockSize,
                          const Volume &volume) override;

    FSStatusCode CreateFsS3(const std::string &fsName, uint64_t blockSize,
                            const S3Info &s3Info) override;

    FSStatusCode DeleteFs(const std::string &fsName) override;

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

 private:
    FSStatusCode ReturnError(int retcode);

 private:
    MDSBaseClient *mdsbasecli_;
    ::curve::client::RPCExcutorRetryPolicy rpcexcutor_;
    ::curve::client::MetaServerOption mdsOpt_;
};

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs


#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_MDS_CLIENT_H_

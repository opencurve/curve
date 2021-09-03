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
 * Created Date: Thur Sept 2 2021
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_CLIENT_RPCCLIENT_METACACHE_H_
#define CURVEFS_SRC_CLIENT_RPCCLIENT_METACACHE_H_

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <unordered_map>

#include <memory>
#include <vector>
#include <string>

#include "curvefs/proto/common.pb.h"
#include "curvefs/src/client/common/common.h"
#include "src/client/metacache_struct.h"
#include "curvefs/src/client/rpcclient/cli2_client.h"
#include "curvefs/src/client/rpcclient/mds_client.h"
#include "curvefs/src/client/common/config.h"

using ::curve::client::CopysetID;
using ::curve::client::CopysetInfo;
using ::curve::client::LogicPoolID;
using ::curve::common::RWLock;
using ::curvefs::client::common::MetaserverID;
using ::curvefs::client::common::MetaServerOpType;
using ::curvefs::client::common::PartitionID;
using ::curvefs::common::PartitionInfo;
using ::curvefs::client::common::MetaCacheOpt;

namespace curvefs {
namespace client {
namespace rpcclient {

struct CopysetGroupID {
    LogicPoolID poolID;
    CopysetID copysetID;

    CopysetGroupID() = default;
    CopysetGroupID(const LogicPoolID &poolid, const CopysetID &copysetid)
        : copysetID(copysetid), poolID(poolid) {}

    std::string ToString() const {
        return "{" + std::to_string(poolID) + "," + std::to_string(copysetID) +
               "}";
    }
};

struct CopysetTarget {
    // copyset id
    CopysetGroupID groupID;
    // partition id
    PartitionID partitionID;

    // leader info
    MetaserverID metaServerID;
    butil::EndPoint endPoint;
};

class MetaCache {
 public:
    void Init(MetaCacheOpt opt, std::shared_ptr<Cli2Client> cli2Client,
              std::shared_ptr<MdsClient> mdsClient) {
        metacacheopt_ = opt;
        cli2Client_ = cli2Client;
        mdsClient_ = mdsClient;
    }

    using PoolIDCopysetID = uint64_t;
    using PatitionInfoList = std::vector<PartitionInfo>;
    using FS2PatitionInfoMap = std::unordered_map<uint32_t, PatitionInfoList>;
    using CopysetInfoMap =
        std::unordered_map<PoolIDCopysetID, CopysetInfo<MetaserverID>>;

    virtual bool GetTarget(uint32_t fsID, uint64_t inodeID,
                           CopysetTarget *target, uint64_t *applyIndex,
                           bool refresh = false);

    virtual bool SelectTarget(uint32_t fsID, CopysetTarget *target,
                              uint64_t *applyIndex);

    virtual void UpdateApplyIndex(const CopysetGroupID &groupID,
                                  uint64_t applyIndex);

    virtual uint64_t GetApplyIndex(const CopysetGroupID &groupID);

    virtual bool IsLeaderMayChange(const CopysetGroupID &groupID);

    virtual void UpdateCopysetInfo(const CopysetGroupID &groupID,
                                   const CopysetInfo<MetaserverID> &csinfo);

    virtual void UpdatePartitionInfo(uint32_t fsID,
                                     const PatitionInfoList &pInfoList);

 private:
    bool UpdateCopysetInfoFromMDS(const CopysetGroupID &groupID,
                                  CopysetInfo<MetaserverID> *targetInfo);

    bool UpdateLeaderInternal(const CopysetGroupID &groupID,
                              CopysetInfo<MetaserverID> *toupdateCopyset);


    bool GetParitionListWihFsID(uint32_t fsID, PatitionInfoList *pinfoList);

    bool GetCopysetIDwithInodeID(const PatitionInfoList &pinfoList,
                                 uint32_t inodeID, CopysetGroupID *groupID,
                                 PartitionID *patitionID);

    bool GetCopysetInfowithCopySetID(const CopysetGroupID &groupID,
                                     CopysetInfo<MetaserverID> *targetInfo);

    void UpdateCopysetInfoIfMatchCurrentLeader(const CopysetGroupID &groupID,
                                               const PeerAddr &leaderAddr);

    static PoolIDCopysetID
    CalcLogicPoolCopysetID(const CopysetGroupID &groupID) {
        return (static_cast<uint64_t>(groupID.poolID) << 32) |
               static_cast<uint64_t>(groupID.copysetID);
    }

 private:
    CURVE_CACHELINE_ALIGNMENT RWLock rwlock4fs2PartitionInfoMap_;
    CURVE_CACHELINE_ALIGNMENT FS2PatitionInfoMap fs2PartitionInfoMap_;
    CURVE_CACHELINE_ALIGNMENT RWLock rwlock4copysetInfoMap_;
    CURVE_CACHELINE_ALIGNMENT CopysetInfoMap copysetInfoMap_;

    MetaCacheOpt metacacheopt_;
    std::shared_ptr<Cli2Client> cli2Client_;
    std::shared_ptr<MdsClient> mdsClient_;
};

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_METACACHE_H_

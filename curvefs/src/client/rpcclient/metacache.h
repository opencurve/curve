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

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

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
using ::curvefs::client::common::MetaCacheOpt;
using ::curvefs::client::common::MetaserverID;
using ::curvefs::client::common::MetaServerOpType;
using ::curvefs::client::common::Mutex;
using ::curvefs::client::common::PartitionID;
using ::curvefs::common::PartitionInfo;
using ::curvefs::common::PartitionStatus;

namespace curvefs {
namespace client {
namespace rpcclient {

struct CopysetGroupID {
    LogicPoolID poolID = 0;
    CopysetID copysetID = 0;

    CopysetGroupID() = default;
    CopysetGroupID(const LogicPoolID &poolid, const CopysetID &copysetid)
        : poolID(poolid), copysetID(copysetid) {}

    std::string ToString() const {
        return "{" + std::to_string(poolID) + "," + std::to_string(copysetID) +
               "}";
    }
};

struct CopysetTarget {
    // copyset id
    CopysetGroupID groupID;
    // partition id
    PartitionID partitionID = 0;
    uint64_t txId = 0;

    // leader info
    MetaserverID metaServerID = 0;
    butil::EndPoint endPoint;

    bool IsValid() const {
        return groupID.poolID != 0 && groupID.copysetID != 0 &&
               partitionID != 0 && txId != 0 && metaServerID != 0 &&
               endPoint.ip != butil::IP_ANY && endPoint.port != 0;
    }

    void Reset() {
        groupID = CopysetGroupID{};
        partitionID = 0;
        txId = 0;
        metaServerID = 0;
        endPoint = butil::EndPoint{};
    }
};

inline std::ostream &operator<<(std::ostream &os, const CopysetGroupID &g) {
    os << "[poolid:" << g.poolID << ", copysetid:" << g.copysetID << "]";
    return os;
}

inline std::ostream &operator<<(std::ostream &os, const CopysetTarget &t) {
    os << "groupid:" << t.groupID << ", partitionid:" << t.partitionID
       << ", txid:" << t.txId << ", serverid:" << t.metaServerID
       << ", endpoint:" << butil::endpoint2str(t.endPoint).c_str();
    return os;
}

class MetaCache {
 public:
    void Init(MetaCacheOpt opt, std::shared_ptr<Cli2Client> cli2Client,
              std::shared_ptr<MdsClient> mdsClient) {
        metacacheopt_ = std::move(opt);
        cli2Client_ = std::move(cli2Client);
        mdsClient_ = std::move(mdsClient);
        init_ = false;
    }

    using PoolIDCopysetID = uint64_t;
    using PartitionInfoList = std::vector<PartitionInfo>;
    using CopysetInfoMap =
        std::unordered_map<PoolIDCopysetID, CopysetInfo<MetaserverID>>;

    virtual void SetTxId(uint32_t partitionId, uint64_t txId);

    virtual bool GetTxId(uint32_t fsId, uint64_t inodeId, uint32_t *partitionId,
                         uint64_t *txId);

    virtual void GetAllTxIds(std::vector<PartitionTxId> *txIds);

    virtual bool GetTarget(uint32_t fsID, uint64_t inodeID,
                           CopysetTarget *target, bool refresh = false);

    virtual bool SelectTarget(uint32_t fsID, CopysetTarget *target);

    virtual bool IsLeaderMayChange(const CopysetGroupID &groupID);

    virtual bool MarkPartitionUnavailable(PartitionID pid);

    virtual void UpdateCopysetInfo(const CopysetGroupID &groupID,
                                   const CopysetInfo<MetaserverID> &csinfo);

    virtual bool GetTargetLeader(CopysetTarget *target, bool refresh = false);

    virtual bool GetPartitionIdByInodeId(uint32_t fsID, uint64_t inodeID,
                                         PartitionID *pid);

    bool RefreshTxId();

    // list or create partitions for fs
    bool ListPartitions(uint32_t fsID);

 private:
    void GetTxId(uint32_t partitionId, uint64_t *txId);
    bool CreatePartitions(int currentNum, PartitionInfoList *newPartitions);
    bool DoListOrCreatePartitions(
        bool list, PartitionInfoList *partitionInfos,
        std::map<PoolIDCopysetID, CopysetInfo<MetaserverID>> *copysetMap);
    void DoAddOrResetPartitionAndCopyset(
        PartitionInfoList partitionInfos,
        std::map<PoolIDCopysetID, CopysetInfo<MetaserverID>> copysetMap,
        bool reset);

    // retry policy
    // TODO(@lixiaocui): rpc service may be split to ServiceHelper
    bool UpdateCopysetInfoFromMDS(const CopysetGroupID &groupID,
                                  CopysetInfo<MetaserverID> *targetInfo);
    bool UpdateLeaderInternal(const CopysetGroupID &groupID,
                              CopysetInfo<MetaserverID> *toupdateCopyset);
    void UpdateCopysetInfoIfMatchCurrentLeader(const CopysetGroupID &groupID,
                                               const PeerAddr &leaderAddr);


    // select a dest parition for inode create
    // TODO(@lixiaocui): select parititon may be need SelectPolicy to support
    // more policies
    bool SelectPartition(CopysetTarget *target);

    // get info from partitionMap or copysetMap
    bool GetCopysetIDwithInodeID(uint64_t inodeID, CopysetGroupID *groupID,
                                 PartitionID *patitionID, uint64_t *txId);

    bool GetCopysetInfowithCopySetID(const CopysetGroupID &groupID,
                                     CopysetInfo<MetaserverID> *targetInfo);


    // key tansform
    static PoolIDCopysetID
    CalcLogicPoolCopysetID(const CopysetGroupID &groupID) {
        return (static_cast<uint64_t>(groupID.poolID) << 32) |
               static_cast<uint64_t>(groupID.copysetID);
    }

 private:
    RWLock txIdLock_;
    std::unordered_map<uint32_t, uint64_t> partitionTxId_;

    RWLock rwlock4Partitions_;
    PartitionInfoList partitionInfos_;
    RWLock rwlock4copysetInfoMap_;
    CopysetInfoMap copysetInfoMap_;

    Mutex createMutex_;

    MetaCacheOpt metacacheopt_;
    std::shared_ptr<Cli2Client> cli2Client_;
    std::shared_ptr<MdsClient> mdsClient_;

    uint32_t fsID_;
    std::atomic_bool init_;
};

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_METACACHE_H_

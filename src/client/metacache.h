/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * File Created: Tuesday, 25th September 2018 2:06:22 pm
 * Author: tongguangxun
 */
#ifndef SRC_CLIENT_METACACHE_H_
#define SRC_CLIENT_METACACHE_H_

#include <set>
#include <string>
#include <unordered_map>

#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/client_metric.h"
#include "src/client/mds_client.h"
#include "src/client/metacache_struct.h"
#include "src/client/service_helper.h"
#include "src/client/unstable_helper.h"
#include "src/common/concurrent/rw_lock.h"

namespace curve {
namespace client {

using curve::common::RWLock;

enum class MetaCacheErrorType {
    OK = 0,
    CHUNKINFO_NOT_FOUND = 1,
    LEADERINFO_NOT_FOUND = 2,
    SERVERLIST_NOT_FOUND = 3,
    UNKNOWN_ERROR
};

class MetaCache {
 public:
    using LogicPoolCopysetID = uint64_t;
    using ChunkInfoMap = std::unordered_map<ChunkID, ChunkIDInfo>;
    using CopysetInfoMap =
        std::unordered_map<LogicPoolCopysetID, CopysetInfo<ChunkServerID>>;
    using ChunkIndexInfoMap = std::unordered_map<ChunkIndex, ChunkIDInfo>;

    MetaCache() = default;
    virtual ~MetaCache() = default;

    /**
     *Initialization function
     * @param: Metacacheopt is the configuration option information for the current Metacache
     * @param: mdsclient is the pointer that communicates with mds.
     *Why does it need to pass in mdsclient here?
     *Because the first role that Metacache plays is to cache information on the MDS side
     *So for low-level users who want to use Metacache's copyset client or chunk closure
     *For example, he only needs to know the Metacache and no longer needs to query information from MDS,
     *After the copyset client or chunk closure fails to send IO, it will retrieve the leader again
     *Then try again. If the leader acquisition is unsuccessful, you need to query the latest information of the current copyset from the mds side,
     *Here, the query mds is encapsulated internally, so that the copyset client and chunk closure are not aware of mds
     */
    void Init(const MetaCacheOption &metaCacheOpt, MDSClient *mdsclient);

    /**
     *Obtain chunk information through chunk index
     * @param: chunkidx queries the ID information corresponding to chunks using index
     * @param: chunkinfo is an outgoing parameter that stores the version information of the chunk
     * @param: Successfully returns OK, otherwise returns UNKNOWN_ ERROR
     */
    virtual MetaCacheErrorType GetChunkInfoByIndex(ChunkIndex chunkidx,
                                                   ChunkIDInfo_t *chunkinfo);

    /**
     * @brief Update cached chunk info by chunk index
     */
    virtual void UpdateChunkInfoByIndex(ChunkIndex cindex,
                                        const ChunkIDInfo &chunkinfo);

    /**
     *When sending data, the sender needs to know the corresponding leader and send it to the corresponding chunkserver
     *If it cannot be obtained, set refresh to true on the peripheral, and then pull the latest one from the chunkserver end
     *Server information, and then update the metacache.
     *If the current copyset's leaderMayChange is set, even if refresh is false, it still needs to
     *Go and retrieve new leader information before continuing to issue IO
     * @param: lpid Logical Pool ID
     * @param: cpid is copysetid
     * @param: The serverId corresponds to the ID information of the chunkserver, which is the output parameter
     * @param: serverAddr is the IP information corresponding to serverid
     * @param: refresh. If it cannot be obtained, set the peripheral refresh to true,
     *Then pull the latest data from the chunkserver end
     * @param: fm for statistical metrics
     * @param: Successfully returns 0, otherwise returns -1
     */
    virtual int GetLeader(LogicPoolID logicPoolId, CopysetID copysetId,
                          ChunkServerID *serverId, butil::EndPoint *serverAddr,
                          bool refresh = false, FileMetric *fm = nullptr);
    /**
     *Update the leader information of a copyset
     * @param logicPoolId Logical Pool ID
     * @param copysetId Copy Group ID
     * @param leaderAddr leader address
     * @return: Successfully returns 0, otherwise returns -1
     */
    virtual int UpdateLeader(LogicPoolID logicPoolId, CopysetID copysetId,
                             const butil::EndPoint &leaderAddr);
    /**
     *Update copyset data information, including serverlist
     * @param: lpid Logical Pool ID
     * @param: cpid is copysetid
     * @param: csinfo is the copyset info to be updated
     */
    virtual void UpdateCopysetInfo(LogicPoolID logicPoolId, CopysetID copysetId,
                                   const CopysetInfo<ChunkServerID> &csinfo);


    /**
     *Update chunk information through chunk id
     * @param: cid is chunkid
     * @param: cininfo is the ID information corresponding to the current chunk
     */
    virtual void UpdateChunkInfoByID(ChunkID cid, const ChunkIDInfo &cidinfo);

    /**
     *Obtain the server list information for the current copyset
     * @param: lpid Logical Pool ID
     * @param: cpid is copysetid
     * @return: The copysetinfo information of the current copyset
     */
    virtual CopysetInfo<ChunkServerID> GetServerList(LogicPoolID logicPoolId,
                                                     CopysetID copysetId);

    /**
     *Convert ID to key for cache
     * @param: lpid Logical Pool ID
     * @param: cpid is copysetid
     * @return: is the current key
     */
    static LogicPoolCopysetID CalcLogicPoolCopysetID(LogicPoolID logicPoolId,
                                                     CopysetID copysetId) {
        return (static_cast<uint64_t>(logicPoolId) << 32) |
               static_cast<uint64_t>(copysetId);
    }

    /**
     * @brief: Mark all chunkservers on the entire server as unstable
     *
     * @param: serverIp The IP address of the server
     * @return: 0 set successfully/-1 set failed
     */
    virtual int SetServerUnstable(const std::string &endPoint);

    /**
     *If there is a problem with the chunkserver where the leader is located, which leads to RPC failure. At this point, this
     *Other leader copysets on chunkserver may also have the same issue, so it is necessary to
     *Notify the leader copyset on the current chunkserver Mainly by setting this copyset
     *The leaderMayChange flag of the copyset will be checked when it issues IO again
     *When this flag position is set, the IO issue requires a leader refresh first,
     *If the leader refresh is successful, the leader MayChange will be reset.
     *SetChunkserverUnstable will traverse all copysets on the current chunkserver
     *And set the leaderMayChange flag for the leader copyset of this chunkserver.
     * @param: csid is the currently unstable chunkserver ID
     */
    virtual void SetChunkserverUnstable(ChunkServerID csid);

    /**
     *Add copyset information for the corresponding chunkserver to the map
     * @param: csid is the current chunkserverid
     * @param: cpid is the ID information of the current copyset
     */
    virtual void AddCopysetIDInfo(ChunkServerID csid,
                                  const CopysetIDInfo &cpid);

    virtual void
    UpdateChunkserverCopysetInfo(LogicPoolID lpid,
                                 const CopysetInfo<ChunkServerID> &cpinfo);

    void UpdateFileInfo(const FInfo &fileInfo) { fileInfo_ = fileInfo; }

    const FInfo *GetFileInfo() const { return &fileInfo_; }

    void UpdateFileEpoch(const FileEpoch& fEpoch) {
        fEpoch_ = fEpoch;
    }

    const FileEpoch* GetFileEpoch() const { return &fEpoch_; }

    uint64_t GetLatestFileSn() const { return fileInfo_.seqnum; }

    void SetLatestFileSn(uint64_t newSn) { fileInfo_.seqnum = newSn; }

    FileStatus GetLatestFileStatus() const { return fileInfo_.filestatus; }

    void SetLatestFileStatus(FileStatus status) {
        fileInfo_.filestatus = status;
    }

    /**
     *Get the LeaderMayChange flag of the corresponding copyset
     */
    virtual bool IsLeaderMayChange(LogicPoolID logicpoolId,
                                   CopysetID copysetId);

    /**
     *Test Usage
     *Obtain copysetinfo information
     */
    virtual CopysetInfo<ChunkServerID> GetCopysetinfo(LogicPoolID lpid,
                                                      CopysetID csid);

    UnstableHelper &GetUnstableHelper() { return unstableHelper_; }

    uint64_t InodeId() const { return fileInfo_.id; }

    /**
     * @brief Get file segment info about the segmentIndex
     */
    FileSegment *GetFileSegment(SegmentIndex segmentIndex);

    /**
     * @brief Clean chunks of this segment
     */
    virtual void CleanChunksInSegment(SegmentIndex segmentIndex);

 private:
    /**
     * @brief Update copyset replication group information from mds
     * @param logicPoolId Logical Pool ID
     * @param copysetId Copy Group ID
     * @return 0 successful/-1 failed
     */
    int UpdateCopysetInfoFromMDS(LogicPoolID logicPoolId, CopysetID copysetId);

    /**
     *Update the leader information of the copyset
     * @param[in]: logicPoolId Logical Pool Information
     * @param[in]: copysetId Copy group information
     * @param[out]: toupdateCopyset is the pointer to the copyset information to be updated in the metacache
     */
    int UpdateLeaderInternal(LogicPoolID logicPoolId, CopysetID copysetId,
                             CopysetInfo<ChunkServerID> *toupdateCopyset,
                             FileMetric *fm = nullptr);

    /**
     *Pull replication group information from MDS, if the current leader is in the replication group
     *Update local cache, otherwise do not update
     * @param: logicPoolId Logical Pool ID
     * @param: copysetId Copy group ID
     * @param: leaderAddr The current leader address
     */
    void UpdateCopysetInfoIfMatchCurrentLeader(LogicPoolID logicPoolId,
                                               CopysetID copysetId,
                                               const PeerAddr &leaderAddr);

 private:
    MDSClient *mdsclient_;
    MetaCacheOption metacacheopt_;

    //Mapping table from chunkindex to chunkidinfo
    CURVE_CACHELINE_ALIGNMENT ChunkIndexInfoMap chunkindex2idMap_;

    CURVE_CACHELINE_ALIGNMENT RWLock rwlock4Segments_;
    CURVE_CACHELINE_ALIGNMENT std::unordered_map<SegmentIndex, FileSegment>
        segments_;  // NOLINT

    //Mapping table for logicalpoolid and copysetid to copysetinfo
    CURVE_CACHELINE_ALIGNMENT CopysetInfoMap lpcsid2CopsetInfoMap_;

    //Chunkid to chunkidinfo mapping table
    CURVE_CACHELINE_ALIGNMENT ChunkInfoMap chunkid2chunkInfoMap_;

    //Three read and write locks protect each of the three mapping tables mentioned above
    CURVE_CACHELINE_ALIGNMENT RWLock rwlock4chunkInfoMap_;
    CURVE_CACHELINE_ALIGNMENT RWLock rwlock4ChunkInfo_;
    CURVE_CACHELINE_ALIGNMENT RWLock rwlock4CopysetInfo_;

    //ChunkserverCopysetIDMap_ Store the mapping from the current chunkserver to the copyset
    //When the rpc closure is set to SetChunkserverUnstable, the chunkserver will be set
    //All copysets of are in the leaderMayChange state, and subsequent copysets need to determine this value to see
    //Do you need to refresh the leader

    //Mapping chunkserverid to copyset
    std::unordered_map<ChunkServerID, std::set<CopysetIDInfo>>
        chunkserverCopysetIDMap_;  // NOLINT
    //Read write lock protection unstableCSMap
    CURVE_CACHELINE_ALIGNMENT RWLock rwlock4CSCopysetIDMap_;

    //Current file information
    FInfo fileInfo_;

    // epoch info
    FileEpoch fEpoch_;

    UnstableHelper unstableHelper_;
};

}  // namespace client
}  // namespace curve
#endif  // SRC_CLIENT_METACACHE_H_

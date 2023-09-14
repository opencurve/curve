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
 * Created Date: 2019-11-28
 * Author: charisu
 */
#ifndef SRC_TOOLS_COPYSET_CHECK_CORE_H_
#define SRC_TOOLS_COPYSET_CHECK_CORE_H_

#include <gflags/gflags.h>
#include <unistd.h>

#include <string>
#include <iostream>
#include <map>
#include <vector>
#include <algorithm>
#include <set>
#include <memory>
#include <iterator>
#include <utility>

#include "proto/topology.pb.h"
#include "src/mds/common/mds_define.h"
#include "src/common/string_util.h"
#include "src/tools/mds_client.h"
#include "src/tools/chunkserver_client.h"
#include "src/tools/metric_name.h"
#include "src/tools/curve_tool_define.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/common/concurrent/concurrent.h"

using curve::mds::topology::PoolIdType;
using curve::mds::topology::CopySetIdType;
using curve::mds::topology::ChunkServerIdType;
using curve::mds::topology::ServerIdType;
using curve::mds::topology::kTopoErrCodeSuccess;
using curve::mds::topology::OnlineState;
using curve::mds::topology::ChunkServerStatus;
using curve::chunkserver::ToGroupId;
using curve::chunkserver::GetPoolID;
using curve::chunkserver::GetCopysetID;
using curve::common::Mutex;
using curve::common::Thread;

namespace curve {
namespace tool {

using CopySet = std::pair<PoolIdType, CopySetIdType>;
using CopySetInfosType = std::vector<std::map<std::string, std::string>>;

enum class CheckResult {
    // copyset Health
    kHealthy = 0,
    // Parsing result failed
    kParseError = -1,
    // The number of peers is less than expected
    kPeersNoSufficient  = -2,
    // The index difference between replicas is too large
    kLogIndexGapTooBig = -3,
    // There is a replica installing the snapshot
    kInstallingSnapshot = -4,
    // A few instances are not online
    kMinorityPeerNotOnline = -5,
    // Most replicas are not online
    kMajorityPeerNotOnline = -6,
    kOtherErr = -7
};

enum class ChunkServerHealthStatus {
    kHealthy = 0,  // All copysets on chunkserver are healthy
    kNotHealthy = -1,  // Copyset on chunkserver is unhealthy
    kNotOnline = -2  // Chunkserver is not online
};

struct CopysetStatistics {
    CopysetStatistics() :
        totalNum(0), unhealthyNum(0), unhealthyRatio(0) {}
    CopysetStatistics(uint64_t total, uint64_t unhealthy);
    uint64_t totalNum;
    uint64_t unhealthyNum;
    double unhealthyRatio;
};

const char kTotal[] = "total";
const char kInstallingSnapshot[] = "installing snapshot";
const char kNoLeader[] = "no leader";
const char kLogIndexGapTooBig[] = "index gap too big";
const char kPeersNoSufficient[] = "peers not sufficient";
const char kMinorityPeerNotOnline[] = "minority peer not online";
const char kMajorityPeerNotOnline[] = "majority peer not online";
const char kThreeCopiesInconsistent[] = "Three copies inconsistent";

class CopysetCheckCore {
 public:
    CopysetCheckCore(std::shared_ptr<MDSClient> mdsClient,
                     std::shared_ptr<ChunkServerClient> csClient = nullptr) :
                     mdsClient_(mdsClient), csClient_(csClient) {}
    virtual ~CopysetCheckCore() = default;

    /**
     * @brief Initialize mds client
     * @param mdsAddr Address of mds, supporting multiple addresses separated by ','
     * @return returns 0 for success, -1 for failure
     */
    virtual int Init(const std::string& mdsAddr);

    /**
    * @brief check health of one copyset
    *
    * @param logicalPoolId
    * @param copysetId
    *
    * @return error code
    */
    virtual CheckResult CheckOneCopyset(const PoolIdType& logicalPoolId,
                        const CopySetIdType& copysetId);

    /**
    * @brief Check the health status of all copysets on a certain chunkserver
    *
    * @param chunkserId chunkserverId
    *
    * @return Health returns 0, unhealthy returns -1
    */
    virtual int CheckCopysetsOnChunkServer(
                            const ChunkServerIdType& chunkserverId);

    /**
    * @brief Check the health status of all copysets on a certain chunkserver
    *
    * @param chunkserAddr chunkserver address
    *
    * @return Health returns 0, unhealthy returns -1
    */
    virtual int CheckCopysetsOnChunkServer(const std::string& chunkserverAddr);

    /**
    * @brief Check copysets on offline chunkservers
    */
    virtual int CheckCopysetsOnOfflineChunkServer();

    /**
    * @brief Check the health status of all copysets on a server
    *
    * @param serverId Server ID
    * @param[out] unhealthyChunkServers optional parameter, a list of unhealthy chunkservers with copyset on the server
    *
    * @return Health returns 0, unhealthy returns -1
    */
    virtual int CheckCopysetsOnServer(const ServerIdType& serverId,
                    std::vector<std::string>* unhealthyChunkServers = nullptr);

    /**
    * @brief Check the health status of all copysets on a server
    *
    * @param serverId IP of server
    * @param[out] unhealthyChunkServers optional parameter, a list of unhealthy chunkservers with copyset on the server
    *
    * @return Health returns 0, unhealthy returns -1
    */
    virtual int CheckCopysetsOnServer(const std::string& serverIp,
                    std::vector<std::string>* unhealthyChunkServers = nullptr);

    /**
    * @brief Check the health status of all copysets in the cluster
    *
    * @return Health returns 0, unhealthy returns -1
    */
    virtual int CheckCopysetsInCluster();

    /**
    * @brief Check the operators in the cluster
    * @param opName The name of the operator
    * @param checkTimeSec check time
    * @return returns 0 if the check is normal, or -1 if the check fails or there is an operator present
    */
    virtual int CheckOperator(const std::string& opName,
                              uint64_t checkTimeSec);

    /**
     * @brief Calculate the proportion of unhealthy copysets, check and call
     * @return The proportion of unhealthy copysets
     */
    virtual CopysetStatistics GetCopysetStatistics();

    /**
     * @brief to obtain a list of copysets, usually called after checking, and then printed out
     * @return List of copysets
     */
    virtual const std::map<std::string, std::set<std::string>>& GetCopysetsRes()
                                                            const {
        return copysets_;
    }

    /**
     * @brief Get copysets info for specified copysets
     */
    virtual void GetCopysetInfos(const char* key,
                        std::vector<CopysetInfo>* copysets);

    /**
     * @brief Get detailed information about copyset
     * @return Details of copyset
     */
    virtual const std::string& GetCopysetDetail() const {
        return copysetsDetail_;
    }

    /**
     * @brief: Obtain a list of chunkservers with service exceptions during the inspection process, which is usually called after the inspection and printed out
     * @return List of chunkservers with service exceptions
     */
    virtual const std::set<std::string>& GetServiceExceptionChunkServer()
                                        const {
        return serviceExceptionChunkServers_;
    }

    /**
    * @brief: Obtain the list of failed chunkservers for copyset during the check process, which is usually called after the check and printed out
    * @return List of chunkservers with copyset loading exceptions
     */
    virtual const std::set<std::string>& GetCopysetLoadExceptionChunkServer()
                                        const {
        return copysetLoacExceptionChunkServers_;
    }

    /**
    * @brief Check if chunkserver is online by sending RPC
    *
    * @param chunkserverAddr Address of chunkserver
    *
    * @return returns true online and false offline
    */
    virtual bool CheckChunkServerOnline(const std::string& chunkserverAddr);

    /**
    * @brief List volumes on majority peers offline copysets
    *
    * @param fileNames affected volumes
    *
    * @return return 0 when sucess, otherwise return -1
    */
    virtual int ListMayBrokenVolumes(std::vector<std::string>* fileNames);

 private:
    /**
    * @brief Analyze the replication group information for the specified groupId from iobuf,
    *        Each replication group's information is placed in a map
    *
    * @param gIds: The groupId of the replication group to be queried. If it is empty, all queries will be performed
    * @param iobuf The iobuf to analyze
    * @param[out] maps A list of copyset information, where each copyset's information is a map
    * @param saveIobufStr Do you want to save the detailed content in iobuf
    *
    */
    void ParseResponseAttachment(const std::set<std::string>& gIds,
                        butil::IOBuf* iobuf,
                        CopySetInfosType* copysetInfos,
                        bool saveIobufStr = false);

    /**
    * @brief Check the health status of all copysets on a certain chunkserver
    *
    * @param chunkserId chunkserverId
    * @param chunkserverAddr chunkserver address, just specify one of the two
    *
    * @return Health returns 0, unhealthy returns -1
    */
    int CheckCopysetsOnChunkServer(const ChunkServerIdType& chunkserverId,
                                   const std::string& chunkserverAddr);

    /**
    * @brief check copysets' healthy status on chunkserver
    *
    * @param[in] chunkserAddr: chunkserver address
    * @param[in] groupIds: groupId for check, default is null, check all the copysets
    * @param[in] queryLeader: whether send rpc to chunkserver which copyset leader on.
    *                    All the chunkserves will be check when check clusters status.
    * @param[in] record: raft state rpc response from chunkserver
    * @param[in] queryCs: whether send rpc to chunkserver
    *
    * @return error code
    */
    ChunkServerHealthStatus CheckCopysetsOnChunkServer(
                            const std::string& chunkserverAddr,
                            const std::set<std::string>& groupIds,
                            bool queryLeader = true,
                            std::pair<int, butil::IOBuf> *record = nullptr,
                            bool queryCs = true);

    /**
    * @brief Check the health status of all copysets on a server
    *
    * @param serverId Server ID
    * @param serverIp Just specify one of the server's IP, serverId, or serverIp
    * @param queryLeader Does the send RPC queries to the server where the leader is located,
    *                    For checking the cluster, all servers will be traversed without querying
    *
    * @return Health returns 0, unhealthy returns -1
    */
    int CheckCopysetsOnServer(const ServerIdType& serverId,
                    const std::string& serverIp,
                    bool queryLeader = true,
                    std::vector<std::string>* unhealthyChunkServers = nullptr);

    /**
     * @brief concurrent check copyset on server
     * @param[in] chunkservers: chunkservers on server
     * @param[in] index: the deal index of chunkserver
     * @param[in] result: rpc response from chunkserver
     */
    void ConcurrentCheckCopysetsOnServer(
                const std::vector<ChunkServerInfo> &chunkservers,
                uint32_t *index,
                std::map<std::string, std::pair<int, butil::IOBuf>> *result);

    /**
    * @brief: Analyze whether the copyset is healthy based on the copyset information in the leader's map, and return 0 if it is healthy. Otherwise
    *         Otherwise, an error code will be returned
    *
    * @param map The copyset information of the leader is stored as key value pairs
    *
    * @return returns an error code
    */
    CheckResult CheckHealthOnLeader(std::map<std::string, std::string>* map);

    /**
    * @brief Initiate raft state rpc to chunkserver
    *
    * @param chunkserverAddr Address of  chunkserver
    * @param[out] iobuf The responseattachment returned by is valid when 0 is returned
    *
    * @return returns 0 for success, -1 for failure
    */
    int QueryChunkServer(const std::string& chunkserverAddr,
                         butil::IOBuf* iobuf);

    /**
    * @brief: Update all copysets on chunkserver to peerNotOnline
    *
    * @param csAddr chunkserver Address of 
    *
    * @return None
    */
    void UpdatePeerNotOnlineCopysets(const std::string& csAddr);

    /**
    * @brief: Using the copyset configuration group in mds as a reference, check if chunkserver is in the copyset configuration group
    *
    * @param csAddr Address of chunkserver
    * @param copysets copyset list
    * @param[out] result check result, copyset mapping to presence or absence
    *
    * @return returns true, otherwise returns false
    */
    int CheckIfChunkServerInCopysets(const std::string& csAddr,
                                     const std::set<std::string> copysets,
                                     std::map<std::string, bool>* result);

    /**
    * @brief Check if the copyset without a leader is healthy
    *
    * @param csAddr chunkserver address
    * @param copysetsPeers copyset's groupId to Peers mapping
    *
    * @return returns true if healthy, false if unhealthy
    */
    bool CheckCopysetsNoLeader(const std::string& csAddr,
                            const std::map<std::string,
                                           std::vector<std::string>>&
                                                copysetsPeers);

    /**
    * @brief Clear Statistics
    *
    * @return None
    */
    void Clear();

    /**
    * @brief: Obtain the online status of the copyset on chunkserver
    *
    * @param csAddr chunkserver address
    * @param groupId copyset's groupId
    *
    * @return returns true online
    */
    bool CheckCopySetOnline(const std::string& csAddr,
                            const std::string& groupId);

    /**
    * @brief: Obtain the number of offline peers
    *
    *
    * @param peers The list of replica peers in the form of ip:port:id
    *
    * @return returns an error code
    */
    CheckResult CheckPeerOnlineStatus(const std::string& groupId,
                                      const std::vector<std::string>& peers);

    /**
    * @brief Update the groupId list of copyset on chunkserver
    *
    * @param csAddr chunkserver address
    * @param copysetInfos copyset information list
    */
    void UpdateChunkServerCopysets(const std::string& csAddr,
                            const CopySetInfosType& copysetInfos);

    int CheckCopysetsWithMds();

    int CheckScanStatus(const std::vector<CopysetInfo>& copysetInfos);

 private:
    // Client sending RPC to mds
    std::shared_ptr<MDSClient> mdsClient_;

    // for unittest mock csClient
    std::shared_ptr<ChunkServerClient> csClient_;

    // Save information for copyset
    std::map<std::string, std::set<std::string>> copysets_;

    // Used to save the chunkservers that failed to send RPC
    std::set<std::string> serviceExceptionChunkServers_;
    // Used to save some copysets and load problematic chunkservers
    std::set<std::string> copysetLoacExceptionChunkServers_;
    // Used to store the copyset list on accessed chunkservers to avoid duplicate RPCs
    std::map<std::string, std::set<std::string>> chunkserverCopysets_;

    // When querying a single copyset, save the detailed information of the replication group
    std::string copysetsDetail_;

    const std::string kEmptyAddr = "0.0.0.0:0:0";

    // mutex for concurrent rpc to chunkserver
    Mutex indexMutex;
    Mutex vectorMutex;
    Mutex mapMutex;
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_COPYSET_CHECK_CORE_H_

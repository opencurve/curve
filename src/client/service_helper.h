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
 * File Created: Wednesday, 26th December 2018 12:28:16 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_SERVICE_HELPER_H_
#define SRC_CLIENT_SERVICE_HELPER_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <stdint.h>
#include <vector>
#include <string>
#include <unordered_set>
#include <memory>
#include "proto/cli2.pb.h"
#include "proto/nameserver2.pb.h"
#include "src/client/client_common.h"
#include "src/client/client_metric.h"
#include "src/client/metacache_struct.h"

namespace curve {
namespace client {

// GetLeader request rpc parameter information
struct GetLeaderRpcOption {
    uint32_t rpcTimeoutMs;

    explicit GetLeaderRpcOption(uint32_t rpcTimeoutMs = 500)
        : rpcTimeoutMs(rpcTimeoutMs) {}
};

// The copyset information and rpc related parameter information corresponding to the GetLeader request
struct GetLeaderInfo {
    LogicPoolID logicPoolId;
    CopysetID   copysetId;
    std::vector<CopysetPeerInfo<ChunkServerID>> copysetPeerInfo;
    int16_t     currentLeaderIndex;
    GetLeaderRpcOption rpcOption;

    GetLeaderInfo(const LogicPoolID& logicPoolId,
                  const CopysetID& copysetId,
                  const std::vector<CopysetPeerInfo<ChunkServerID>>& copysetPeerInfo, //NOLINT
                  int16_t currentLeaderIndex,
                  const GetLeaderRpcOption& rpcOption = GetLeaderRpcOption())
      : logicPoolId(logicPoolId),
        copysetId(copysetId),
        copysetPeerInfo(copysetPeerInfo),
        currentLeaderIndex(currentLeaderIndex),
        rpcOption(rpcOption) {}
};

class GetLeaderProxy;

// GetLeader asynchronous request callback
struct GetLeaderClosure : public google::protobuf::Closure {
    GetLeaderClosure(LogicPoolID logicPoolId, CopysetID copysetId,
                     std::shared_ptr<GetLeaderProxy> proxy)
        : logicPoolId(logicPoolId), copysetId(copysetId), proxy(proxy) {}

    void Run() override;

    LogicPoolID logicPoolId;
    CopysetID copysetId;
    std::shared_ptr<GetLeaderProxy> proxy;

    brpc::Controller cntl;
    curve::chunkserver::GetLeaderResponse2 response;
};

// ServiceHelper is a tool for client-side RPC services
class ServiceHelper {
 public:
    /**
     * transfer FileInfo to FInfo_t and FileEpoch_t
     * @param: finfo  file info of FileInfo type
     * @param: fi File  info of FInfo_t type
     * @param: fEpoch  file Epoch info
     */
    static void ProtoFileInfo2Local(const curve::mds::FileInfo& finfo,
                                    FInfo_t* fi, FileEpoch_t* fEpoch);

    static void ProtoCloneSourceInfo2Local(
        const curve::mds::OpenFileResponse& openFileResponse,
        CloneSourceInfo* info);

    /**
     * Obtain the latest leader information from the chunkserver side
     * @param[in]: getLeaderInfo is the information of the corresponding copyset
     * @param[out]: leaderAddr is the output parameter that returns the leader information of the current copyset
     * @param[out]: leaderId is the output parameter, returning the ID information of the current leader
     * @param[in]: fileMetric is a record used for metric
     * @return: Successfully returns 0, otherwise returns -1
     */
    static int GetLeader(const GetLeaderInfo& getLeaderInfo,
                         PeerAddr *leaderAddr,
                         ChunkServerID* leaderId = nullptr,
                         FileMetric* fileMetric = nullptr);
    /**
     * Obtain user information from the file name
     * The user information needs to be included in the file name, such as the file name being temp and the username being user,
     * So the complete file information is: temp_user_.
     * If the file name is: /temp_temp_, So the complete file name is /temp_temp__user_.
     * @param[in]: filename is the file name passed down by the user
     * @param[out]: realfilename is the true file name
     * @param[out]: user information, output parameters
     * @return: Obtained user information as true, otherwise false
     */
    static bool GetUserInfoFromFilename(const std::string& fname,
                                        std::string* realfilename,
                                        std::string* user);

    /**
     * @brief: Send an HTTP request to determine if the chunkserver is healthy
     *
     * @param: endPoint chunkserver's ip:port
     * @param: HTTP request timeout
     *
     * @return: 0 indicates health, -1 indicates unhealthy
     */
    static int CheckChunkServerHealth(const butil::EndPoint& endPoint,
                                      int32_t requestTimeoutMs);

    static common::ReadWriteThrottleParams ProtoFileThrottleParamsToLocal(
        const curve::mds::FileThrottleParams& params);

    static void ParseProtoThrottleParams(
        const curve::mds::ThrottleParams& params,
        common::ReadWriteThrottleParams* localParams);
};

}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_SERVICE_HELPER_H_

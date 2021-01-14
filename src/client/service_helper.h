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

// GetLeader请求rpc参数信息
struct GetLeaderRpcOption {
    uint32_t rpcTimeoutMs;

    explicit GetLeaderRpcOption(uint32_t rpcTimeoutMs = 500)
        : rpcTimeoutMs(rpcTimeoutMs) {}
};

// GetLeader请求对应的copyset信息及rpc相关参数信息
struct GetLeaderInfo {
    LogicPoolID logicPoolId;
    CopysetID   copysetId;
    std::vector<CopysetPeerInfo> copysetPeerInfo;
    int16_t     currentLeaderIndex;
    GetLeaderRpcOption rpcOption;

    GetLeaderInfo(const LogicPoolID& logicPoolId,
                  const CopysetID& copysetId,
                  const std::vector<CopysetPeerInfo>& copysetPeerInfo,
                  int16_t currentLeaderIndex,
                  const GetLeaderRpcOption& rpcOption = GetLeaderRpcOption())
      : logicPoolId(logicPoolId),
        copysetId(copysetId),
        copysetPeerInfo(copysetPeerInfo),
        currentLeaderIndex(currentLeaderIndex),
        rpcOption(rpcOption) {}
};

class GetLeaderProxy;

// GetLeader异步请求回调
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

// ServiceHelper是client端RPC服务的一些工具
class ServiceHelper {
 public:
    /**
     * proto格式的FInfo转换为本地格式的FInfo
     * @param: finfo为proto格式的文件信息
     * @param: fi为本地格式的文件信息
     */
    static void ProtoFileInfo2Local(const curve::mds::FileInfo& finfo,
                                    FInfo_t* fi);

    static void ProtoCloneSourceInfo2Local(
        const curve::mds::OpenFileResponse& openFileResponse,
        CloneSourceInfo* info);

    /**
     * 从chunkserver端获取最新的leader信息
     * @param[in]: getLeaderInfo为对应copyset的信息
     * @param[out]: leaderAddr是出参，返回当前copyset的leader信息
     * @param[out]: leaderId是出参，返回当前leader的id信息
     * @param[in]: fileMetric是用于metric的记录
     * @return: 成功返回0，否则返回-1
     */
    static int GetLeader(const GetLeaderInfo& getLeaderInfo,
                         ChunkServerAddr *leaderAddr,
                         ChunkServerID* leaderId = nullptr,
                         FileMetric* fileMetric = nullptr);
    /**
     * 从文件名中获取user信息.
     * 用户的user信息需要夹在文件名中，比如文件名为temp,用户名为user,
     * 那么其完整的文件信息是:temp_user_。
     * 如果文件名为: /temp_temp_,那么完整文件名为/temp_temp__user_。
     * @param[in]: filename为用户传下来的文件名
     * @param[out]:realfilename是真正文件名
     * @param[out]: user信息,出参
     * @return: 获取到user信息为true，否则false
     */
    static bool GetUserInfoFromFilename(const std::string& fname,
                                        std::string* realfilename,
                                        std::string* user);

    /**
     * @brief: 发送http请求，判断chunkserver是否健康
     *
     * @param: endPoint chunkserver的ip:port
     * @param: http请求的超时时间
     *
     * @return: 0 表示健康，-1表示不健康
     */
    static int CheckChunkServerHealth(const butil::EndPoint& endPoint,
                                      int32_t requestTimeoutMs);
};

}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_SERVICE_HELPER_H_

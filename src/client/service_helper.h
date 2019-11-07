/*
 * Project: curve
 * File Created: Wednesday, 26th December 2018 12:28:16 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_SERVICE_HELPER_H_
#define SRC_CLIENT_SERVICE_HELPER_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <stdint.h>
#include <vector>
#include <string>
#include <unordered_set>
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
    uint32_t backupRequestMs;
    std::string backupRequestLbName;

    explicit GetLeaderRpcOption(
        uint32_t rpcTimeoutMs = 500,
        uint32_t backupRequestMs = 100,
        const std::string& backupRequestLbName = "rr")
      : rpcTimeoutMs(rpcTimeoutMs),
        backupRequestMs(backupRequestMs),
        backupRequestLbName(backupRequestLbName) {}
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

// ServiceHelper是client端RPC服务的一些工具
class ServiceHelper {
 public:
    /**
     * proto格式的FInfo转换为本地格式的FInfo
     * @param: finfo为proto格式的文件信息
     * @param: fi为本地格式的文件信息
     */
    static void ProtoFileInfo2Local(const curve::mds::FileInfo* finfo,
                                    FInfo_t* fi);
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
                         FileMetric_t* fileMetric = nullptr);
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
     * 从chunkserver端获取最新的leader信息
     * @param[in]: getLeaderInfo为对应copyset的信息
     * @param[in]: chunkserverIpPorts为需要发送请求的chunkserver
     * @param[in]: fileMetric是用于metric的记录
     * @param[out]: leaderAddr是出参，返回当前copyset的leader信息
     * @param[out]: leaderId是出参，返回当前leader的id信息
     * @param[out]: cntlErrCode是出参，返回cntl失败时的状态码
     * @param[out]: failedAddr是出参，返回cntl失败时对应的server addr
     * @return: 成功返回0，否则返回-1
     */
    static int GetLeaderInternal(
       const GetLeaderInfo& getLeaderInfo,
       const std::unordered_set<std::string>& chunkserverIpPorts,  // NOLINT
       FileMetric* fileMetric,
       ChunkServerAddr* leaderAddr,
       ChunkServerID* leaderId,
       int* cntlErrCode,
       std::string* cntlFailedAddr);

    /**
     * 以chunkserverIpPorts构造用于初始化channel的naming service url
     * @param: chunkserverIpPorts存放多个ip:port字符串
     * @return: 构造的url 例如：list://127.0.0.1:12345,127.0.0.1:12346,
     */
    static std::string BuildChannelUrl(
      const std::unordered_set<std::string>& chunkserverIpPorts);
};

}   // namespace client
}   // namespace curve
#endif  // SRC_CLIENT_SERVICE_HELPER_H_

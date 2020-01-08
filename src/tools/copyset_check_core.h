/*
 * Project: curve
 * Created Date: 2019-11-28
 * Author: charisu
 * Copyright (c) 2018 netease
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

#include "proto/topology.pb.h"
#include "src/mds/common/mds_define.h"
#include "src/common/string_util.h"
#include "src/tools/mds_client.h"
#include "src/tools/chunkserver_client.h"
#include "src/tools/metric_name.h"

using curve::mds::topology::PoolIdType;
using curve::mds::topology::CopySetIdType;
using curve::mds::topology::ChunkServerIdType;
using curve::mds::topology::ServerIdType;
using curve::mds::topology::kTopoErrCodeSuccess;
using curve::mds::topology::OnlineState;
using curve::mds::topology::ChunkServerStatus;

namespace curve {
namespace tool {
enum class CheckResult {
    // copyset健康
    kHealthy = 0,
    // 解析结果失败
    kParseError = -1,
    // peer数量小于预期
    kPeersNoSufficient  = -2,
    // 副本间的index差距太大
    kLogIndexGapTooBig = -3,
    // 有副本在安装快照
    kInstallingSnapshot = -4,
    // 有副本不在线
    kPeerNotOnline = -5
};

enum class ChunkServerHealthStatus {
    kHealthy = 0,  // chunkserver上所有copyset健康
    kNotHealthy = -1,  // chunkserver上有copyset不健康
    kNotOnline = -2  // chunkserver不在线
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
const char kPeerNotOnline[] = "peer not online";

class CopysetCheckCore {
 public:
    CopysetCheckCore(std::shared_ptr<MDSClient> mdsClient,
                     std::shared_ptr<ChunkServerClient> csClient) :
                        mdsClient_(mdsClient), csClient_(csClient) {}
    virtual ~CopysetCheckCore() = default;

    /**
    * @brief 检查单个copyset的健康状态
    *
    * @param logicalPoolId 逻辑池Id
    * @param copysetId 复制组Id
    *
    * @return 健康返回0，不健康返回-1
    */
    virtual int CheckOneCopyset(const PoolIdType& logicalPoolId,
                        const CopySetIdType& copysetId);

    /**
    * @brief 检查某个chunkserver上的所有copyset的健康状态
    *
    * @param chunkserId chunkserverId
    *
    * @return 健康返回0，不健康返回-1
    */
    virtual int CheckCopysetsOnChunkServer(
                            const ChunkServerIdType& chunkserverId);

    /**
    * @brief 检查某个chunkserver上的所有copyset的健康状态
    *
    * @param chunkserAddr chunkserver地址
    *
    * @return 健康返回0，不健康返回-1
    */
    virtual int CheckCopysetsOnChunkServer(const std::string& chunkserverAddr);

    /**
    * @brief 检查某个server上的所有copyset的健康状态
    *
    * @param serverId server的id
    * @param[out] unhealthyChunkServers 可选参数，server上copyset不健康的chunkserver的列表
    *
    * @return 健康返回0，不健康返回-1
    */
    virtual int CheckCopysetsOnServer(const ServerIdType& serverId,
                    std::vector<std::string>* unhealthyChunkServers = nullptr);

    /**
    * @brief 检查某个server上的所有copyset的健康状态
    *
    * @param serverId server的ip
    * @param[out] unhealthyChunkServers 可选参数，server上copyset不健康的chunkserver的列表
    *
    * @return 健康返回0，不健康返回-1
    */
    virtual int CheckCopysetsOnServer(const std::string& serverIp,
                    std::vector<std::string>* unhealthyChunkServers = nullptr);

    /**
    * @brief 检查集群中所有copyset的健康状态
    *
    * @return 健康返回0，不健康返回-1
    */
    virtual int CheckCopysetsInCluster();

    /**
     *  @brief 计算不健康的copyset的比例，检查后调用
     *  @return 不健康的copyset的比例
     */
    virtual CopysetStatistics GetCopysetStatistics();

    /**
     *  @brief 获取copyset的列表，通常检查后会调用，然后打印出来
     *  @return copyset的列表
     */
    virtual const std::map<std::string, std::set<std::string>>& GetCopysetsRes()
                                                            const {
        return copysets_;
    }

    /**
     *  @brief 获取copyset的详细信息
     *  @return copyset的详细信息
     */
    virtual const std::string& GetCopysetDetail() const {
        return copysetsDetail_;
    }

    /**
     *  @brief 获取检查过程中服务异常的chunkserver列表，通常检查后会调用，然后打印出来
     *  @return 服务异常的chunkserver的列表
     */
    virtual const std::set<std::string>& GetServiceExceptionChunkServer()
                                        const {
        return serviceExceptionChunkServers_;
    }

 private:
    /**
    * @brief 将逻辑池Id和copyset Id转换成groupId
    *
    * @param logicalPoolId 逻辑池Id
    * @param copysetId 复制组Id
    *
    * @return 返回groupId
    */
    std::string ToGroupId(const PoolIdType& logicalPoolId,
                          const CopySetIdType& copysetId);

    /**
    * @brief 从iobuf分析出指定groupId的复制组的信息，
    *        每个复制组的信息都放到一个map里面
    *
    * @param gIds 要查询的复制组的groupId，为空的话全部查询
    * @param iobuf 要分析的iobuf
    * @param[out] maps copyset信息的列表，每个copyset的信息都是一个map
    *
    */
    void ParseResponseAttachment(const std::set<std::string>& gIds,
                        butil::IOBuf* iobuf,
                        std::vector<std::map<std::string, std::string>>* maps);

    /**
    * @brief 检查某个chunkserver上的所有copyset的健康状态
    *
    * @param chunkserId chunkserverId
    * @param chunkserverAddr chunkserver的地址，两者指定一个就好
    *
    * @return 健康返回0，不健康返回-1
    */
    int CheckCopysetsOnChunkServer(const ChunkServerIdType& chunkserverId,
                                   const std::string& chunkserverAddr);

    /**
    * @brief 检查某个chunkserver上的copyset的健康状态
    *
    * @param chunkserAddr chunkserver的地址
    * @param groupIds 要检查的复制组的groupId,默认为空，全部检查
    * @param queryLeader 是否向leader所在的chunkserver发送RPC查询，
    *              对于检查cluster来说，所有chunkserver都会遍历到，不用查询
    *
    * @return 返回错误码
    */
    ChunkServerHealthStatus CheckCopysetsOnChunkServer(
                                   const std::string& chunkserverAddr,
                                   const std::set<std::string>& groupIds,
                                   bool queryLeader = true);

    /**
    * @brief 检查某个server上的所有copyset的健康状态
    *
    * @param serverId server的id
    * @param serverIp server的ip，serverId或serverIp指定一个就好
    * @param queryLeader 是否向leader所在的server发送RPC查询，
    *              对于检查cluster来说，所有server都会遍历到，不用查询
    *
    * @return 健康返回0，不健康返回-1
    */
    int CheckCopysetsOnServer(const ServerIdType& serverId,
                    const std::string& serverIp,
                    bool queryLeader = true,
                    std::vector<std::string>* unhealthyChunkServers = nullptr);

    /**
    * @brief 根据leader的map里面的copyset信息分析出copyset是否健康，健康返回0，否则
    *        否则返回错误码
    *
    * @param map leader的copyset信息，以键值对的方式存储
    *
    * @return 返回错误码
    */
    CheckResult CheckHealthOnLeader(std::map<std::string, std::string>* map);

    /**
    * @brief 向chunkserver发起raft state rpc
    *
    * @param chunkserverAddr chunkserver的地址
    * @param[out] iobuf 返回的responseattachment，返回0的时候有效
    *
    * @return 成功返回0，失败返回-1
    */
    int QueryChunkServer(const std::string& chunkserverAddr,
                         butil::IOBuf* iobuf);

    /**
    * @brief 通过发送RPC检查chunkserver是否在线
    *
    * @param chunkserverAddr chunkserver的地址
    *
    * @return 在线返回true，不在线返回false
    */
    bool CheckChunkServerOnline(const std::string& chunkserverAddr);

    /**
    * @brief 把chunkserver上所有的copyset更新到peerNotOnline里面
    *
    * @param csAddr chunkserver的地址
    *
    * @return 无
    */
    void UpdatePeerNotOnlineCopysets(const std::string& csAddr);

    /**
    * @brief 清空统计信息
    *
    * @return 无
    */
    void Clear();

    // 向mds发送RPC的client
    std::shared_ptr<MDSClient> mdsClient_;

    // 向chunkserver发送RPC的client
    std::shared_ptr<ChunkServerClient> csClient_;

    // 保存copyset的信息
    std::map<std::string, std::set<std::string>> copysets_;

    // 用来保存发送RPC失败的那些chunkserver
    std::set<std::string> serviceExceptionChunkServers_;
    // 用来存放访问过的chunkserver的在线状态，避免重复RPC
    std::map<std::string, bool> chunkserverStatus_;

    // 查询单个copyset的时候，保存复制组的详细信息
    std::string copysetsDetail_;

    const std::string kOperatorMetricName_ =
                    "mds_scheduler_metric_operator_num";
    const std::string kEmptyAddr = "0.0.0.0:0:0";
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_COPYSET_CHECK_CORE_H_

/*
 * Project: curve
 * Created Date: 2019-10-30
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_COPYSET_CHECK_H_
#define SRC_TOOLS_COPYSET_CHECK_H_

#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <braft/builtin_service.pb.h>

#include <string>
#include <iostream>
#include <map>
#include <vector>
#include <algorithm>
#include <set>
#include <memory>

#include "proto/topology.pb.h"
#include "src/mds/common/mds_define.h"
#include "src/common/string_util.h"
#include "src/common/net_common.h"

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
    kHealthy = 0,  // copyset健康
    kParseError = -1,  // 解析结果失败
    kPeersNoSufficient  = -2,  // peer数量小于预期
    kLogIndexGapTooBig = -3,  // 副本间的index差距太大
    kInstallingSnapshot = -4  // 有副本在安装快照
};

class CopysetCheck {
 public:
    CopysetCheck();
    ~CopysetCheck();

    /**
     *  @brief 根据flag检查复制组健康状态
     *  复制组健康的标准，没有任何副本处于以下状态：
     *  1、复制组的peer数量小于预期
     *  2、副本间log index差距太大
     *  3、有副本在安装快照
     *  4、有副本没有leader
     *  @return 成功返回0，失败返回-1
     */
    int RunCommand(std::string command);

    /**
     *  @brief 打印帮助信息
     */
    void PrintHelp(std::string command);

    /**
     *  @brief 初始化channel
     *  @param mdsAddr mds的地址，支持多地址，用","分隔
     *  @return 无
     */
    int Init(const std::string& mdsAddr);

    /**
     *  @brief 释放资源
     */
    void UnInit(const std::string& mdsAddr);

 private:
    /**
    * @brief 检查单个copyset的健康状态
    *
    * @param logicalPoolId 逻辑池Id
    * @param copysetId 复制组Id
    *
    * @return 健康返回0，不健康返回-1
    */
    int CheckOneCopyset(const PoolIdType& logicalPoolId,
                        const CopySetIdType& copysetId);

    /**
    * @brief 检查某个chunkserver上的所有copyset的健康状态
    *
    * @param chunkserId chunkserverId
    *
    * @return 健康返回0，不健康返回-1
    */
    int CheckCopysetsOnChunkserver(const ChunkServerIdType& chunkserverId);

    /**
    * @brief 检查某个chunkserver上的所有copyset的健康状态
    *
    * @param chunkserId chunkserverId
    *
    * @return 健康返回0，不健康返回-1
    */
    int CheckCopysetsOnChunkserver(const std::string& chunkserverAddr);

    /**
    * @brief 检查某个chunkserver上的所有copyset的健康状态
    *
    * @param chunkserId chunkserverId
    * @param chunkserverAddr chunkserver的地址，两者指定一个就好
    *
    * @return 健康返回0，不健康返回-1
    */
    int CheckCopysetsOnChunkserver(const ChunkServerIdType& chunkserverId,
                                   const std::string& chunkserverAddr);

    /**
    * @brief 检查某个chunkserver上的copyset的健康状态
    *
    * @param chunkserAddr chunkserver的地址
    * @param groupIds 要检查的复制组的groupId,默认为空，全部检查
    * @param queryLeader 是否向leader所在的chunkserver发送RPC查询，
    *              对于检查cluster来说，所有chunkserver都会遍历到，不用查询
    *
    * @return 健康返回0，不健康返回-1
    */
    int CheckCopysetsOnChunkserver(const std::string& chunkserverAddr,
                                   const std::set<std::string>& groupIds,
                                   bool queryLeader = true);

    /**
    * @brief 检查某个server上的所有copyset的健康状态
    *
    * @param serverId server的id
    *
    * @return 健康返回0，不健康返回-1
    */
    int CheckCopysetsOnServer(const ServerIdType& serverId);

    /**
    * @brief 检查某个server上的所有copyset的健康状态
    *
    * @param serverId server的ip
    *
    * @return 健康返回0，不健康返回-1
    */
    int CheckCopysetsOnServer(const std::string serverIp);

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
                              const std::string serverIp,
                              bool queryLeader = true);

    /**
    * @brief 检查集群中所有copyset的健康状态
    *
    * @return 健康返回0，不健康返回-1
    */
    int CheckCopysetsInCluster();

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
    int QueryChunkserver(const std::string& chunkserverAddr,
                         butil::IOBuf* iobuf);


    // 打印copyset检查的详细结果
    void PrintDetail(const std::string& command);
    void PrintVec(const std::vector<std::string>& vec);

    // 向mds发送RPC的channel
    brpc::Channel* channelToMds_;

    // mds的地址
    std::vector<std::string> mdsAddrVec_;

    // 用来保存正在安装快照的copyset
    std::vector<std::string> installSnapshotCopysets_;
    // 用来保存没有leader的copyset
    std::vector<std::string> noLeaderCopysets_;
    // 用来保存日志差距大的copyset
    std::vector<std::string> indexGapBigCopysets_;
    // 用来保存peers数量小于3的copyset
    std::vector<std::string> peerLessCopysets_;
    // 用来保存有问题的chunkserver
    std::vector<std::string> unhealthyChunkservers_;
    // 用来保存有问题的server
    std::vector<std::string> unhealthyServers_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_COPYSET_CHECK_H_

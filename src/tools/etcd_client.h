/*
 * Project: curve
 * File Created: 2019-12-03
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_TOOLS_ETCD_CLIENT_H_
#define SRC_TOOLS_ETCD_CLIENT_H_

#include <brpc/channel.h>
#include <json/json.h>

#include <iostream>
#include <string>
#include <vector>
#include <map>

#include "src/common/string_util.h"
#include "src/tools/version_tool.h"

namespace curve {
namespace tool {

const char kEtcdStatusUri[] = "/v3/maintenance/status";
const char kEtcdVersionUri[] = "/version";
const char kEtcdLeader[] = "leader";
const char kEtcdHeader[] = "header";
const char kEtcdMemberId[] = "member_id";
const char kEtcdCluster[] = "etcdcluster";

class EtcdClient {
 public:
    virtual ~EtcdClient() = default;

    /**
     *  @brief 初始化etcdAddrVec
     *  @param etcdAddr etcd的地址，支持多地址，用","分隔
     *  @return 成功返回0，失败返回-1
     */
    virtual int Init(const std::string& etcdAddr);

    /**
     *  @brief 获取etcd集群的leader
     *  @param[out] leaderAddrVec etcd的leader的地址列表,返回值为0时有效
     *  @param[out] onlineState etcd集群中每个节点的在线状态，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetEtcdClusterStatus(std::vector<std::string>* leaderAddrVec,
                        std::map<std::string, bool>* onlineState);

    /**
     *  @brief 获取etcd的版本并检查版本一致性
     *  @param[out] version 版本
     *  @param[out] failedList 查询version失败的地址列表
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetAndCheckEtcdVersion(std::string* version,
                                       std::vector<std::string>* failedList);

 private:
    std::vector<std::string> etcdAddrVec_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_ETCD_CLIENT_H_

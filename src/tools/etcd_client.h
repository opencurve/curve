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

namespace curve {
namespace tool {
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
     *  @param[out] leaderAddr etcd的leader的地址,返回值为0时有效
     *  @param[out] onlineState etcd集群中每个节点的在线状态，返回值为0时有效
     *  @return 成功返回0，失败返回-1
     */
    virtual int GetEtcdClusterStatus(std::string* leaderAddr,
                        std::map<std::string, bool>* onlineState);
 private:
    std::vector<std::string> etcdAddrVec_;

    const std::string statusUri_ = "/v3/maintenance/status";
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_ETCD_CLIENT_H_

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
 * File Created: 2019-12-03
 * Author: charisu
 */

#ifndef SRC_TOOLS_ETCD_CLIENT_H_
#define SRC_TOOLS_ETCD_CLIENT_H_

#include <brpc/channel.h>
#include <json/json.h>

#include <iostream>
#include <map>
#include <string>
#include <vector>

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
     * @brief Initialize etcdAddrVec
     * @param etcdAddr etcd addresses, supporting multiple addresses separated
     * by ','
     * @return returns 0 for success, -1 for failure
     */
    virtual int Init(const std::string& etcdAddr);

    /**
     * @brief Get the leader of the ETCD cluster
     * @param[out] leaderAddrVec The address list of the leader for etcd, valid
     * when the return value is 0
     * @param[out] onlineState etcd The online state of each node in the
     * cluster, valid when the return value is 0
     * @return returns 0 for success, -1 for failure
     */
    virtual int GetEtcdClusterStatus(std::vector<std::string>* leaderAddrVec,
                                     std::map<std::string, bool>* onlineState);

    /**
     * @brief Get the version of ETCD and check version consistency
     * @param[out] version Version
     * @param[out] failedList Query address list for version failure
     * @return returns 0 for success, -1 for failure
     */
    virtual int GetAndCheckEtcdVersion(std::string* version,
                                       std::vector<std::string>* failedList);

 private:
    std::vector<std::string> etcdAddrVec_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_ETCD_CLIENT_H_

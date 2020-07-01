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
 * Created Date: 2020-03-17
 * Author: charisu
 */

#ifndef SRC_TOOLS_SNAPSHOT_CLONE_CLIENT_H_
#define SRC_TOOLS_SNAPSHOT_CLONE_CLIENT_H_

#include <memory>
#include <vector>
#include <map>
#include <string>

#include "src/tools/metric_client.h"
#include "src/tools/metric_name.h"

namespace curve {
namespace tool {

class SnapshotCloneClient {
 public:
    explicit SnapshotCloneClient(std::shared_ptr<MetricClient> metricClient) :
                            metricClient_(metricClient) {}
    virtual ~SnapshotCloneClient() = default;

    /**
     *  @brief 初始化，从字符串解析出地址和dummy port
     *  @param serverAddr snapshot clone server的地址，支持多地址，用","分隔
     *  @param dummyPort dummy port列表，只输入一个的话
     *         所有server用同样的dummy port，用字符串分隔有多个的话
     *         为每个server设置不同的dummy port
     *  @return 成功返回0，失败返回-1
     */
    virtual int Init(const std::string& serverAddr,
                     const std::string& dummyPort);

    /**
     *  @brief 获取当前服务的snapshot clone server的地址
     */
    virtual std::vector<std::string> GetActiveAddrs();

    /**
     *  @brief 获取snapshot clone server的在线状态
     *          dummyserver在线且dummyserver记录的listen addr
     *          与服务地址一致才认为在线
     *  @param[out] onlineStatus 每个节点的在线状态
     */
    virtual void GetOnlineStatus(std::map<std::string, bool>* onlineStatus);

    virtual const std::map<std::string, std::string>& GetDummyServerMap()
                                                        const {
        return dummyServerMap_;
    }

 private:
    /**
     *  @brief 初始化dummy server地址
     *  @param dummyPort dummy server端口列表
     *  @return 成功返回0，失败返回-1
     */
    int InitDummyServerMap(const std::string& dummyPort);

    /**
     *  @brief 通过dummyServer获取server的监听地址
     *  @param dummyAddr dummyServer的地址
     *  @param[out] listenAddr 服务地址
     *  @return 成功返回0，失败返回-1
     */
    int GetListenAddrFromDummyPort(const std::string& dummyAddr,
                                   std::string* listenAddr);

 private:
    // 用于获取metric
    std::shared_ptr<MetricClient> metricClient_;
    // 保存server地址的vector
    std::vector<std::string> serverAddrVec_;
    // 保存server地址对应的dummy server的地址
    std::map<std::string, std::string> dummyServerMap_;
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_SNAPSHOT_CLONE_CLIENT_H_

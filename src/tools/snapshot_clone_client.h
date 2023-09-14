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
     * @brief initialization, parsing the address and dummy port from the string
     * @param serverAddr Address of snapshot clone server, supporting multiple addresses separated by ','
     * @param dummyPort dummyPort list, if only one is entered
     *        All servers use the same dummy port, separated by strings if there are multiple
     *        Set different dummy ports for each server
     * @return
     *   Success: 0
     *   Failed: -1
     *   No snapshot server: 1
     *
     */
    virtual int Init(const std::string& serverAddr,
                     const std::string& dummyPort);

    /**
     * @brief Get the address of the snapshot clone server for the current service
     */
    virtual std::vector<std::string> GetActiveAddrs();

    /**
     * @brief Get the online status of the snapshot clone server
     *          dummyserver is online and the dummyserver records a listen addr
     *          Only when consistent with the service address is considered online
     * @param[out] onlineStatus The online status of each node
     */
    virtual void GetOnlineStatus(std::map<std::string, bool>* onlineStatus);

    virtual const std::map<std::string, std::string>& GetDummyServerMap()
                                                        const {
        return dummyServerMap_;
    }

 private:
    /**
     * @brief Initialize dummy server address
     * @param dummyPort dummy server port list
     * @return returns 0 for success, -1 for failure
     */
    int InitDummyServerMap(const std::string& dummyPort);

    /**
     * @brief: Obtain the listening address of the server through dummyServer
     * @param dummyAddr Address of dummyServer
     * @param[out] listenAddr service address
     * @return returns 0 for success, -1 for failure
     */
    int GetListenAddrFromDummyPort(const std::string& dummyAddr,
                                   std::string* listenAddr);

 private:
    // Used to obtain metric
    std::shared_ptr<MetricClient> metricClient_;
    // Save the vector of the server address
    std::vector<std::string> serverAddrVec_;
    // Save the address of the dummy server corresponding to the server address
    std::map<std::string, std::string> dummyServerMap_;
};

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_SNAPSHOT_CLONE_CLIENT_H_

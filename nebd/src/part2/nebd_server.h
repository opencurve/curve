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
 * Project: nebd
 * Created Date: 2020-01-16
 * Author: lixiaocui
 */

#ifndef NEBD_SRC_PART2_NEBD_SERVER_H_
#define NEBD_SRC_PART2_NEBD_SERVER_H_

#include <brpc/server.h>
#include <string>
#include <memory>
#include "nebd/src/common/configuration.h"
#include "nebd/src/part2/file_manager.h"
#include "nebd/src/part2/heartbeat_manager.h"
#include "nebd/src/part2/request_executor_curve.h"

namespace nebd {
namespace server {

using ::nebd::common::Configuration;
using ::curve::client::CurveClient;

class NebdServer {
 public:
    NebdServer() {}
    virtual ~NebdServer() {}

    int Init(const std::string &confPath,
        std::shared_ptr<CurveClient> curveClient =
        std::make_shared<CurveClient>());

    int RunUntilAskedToQuit();

    int Fini();

 private:
    /**
     * @brief Load configuration items from the configuration file
     * @param[in] confPath Configuration file path
     * @return false - Failed to load configuration file true - Successfully loaded configuration file
     */
    bool LoadConfFromFile(const std::string &confPath);

    /**
     * @brief Initialize NebdFileManager
     * @return false - initialization failed true - initialization successful
     */
    bool InitFileManager();

    /**
     * @brief initialization request_ Executor_ Curve
     * @return false - initialization failed true - initialization successful
     */
    bool InitCurveRequestExecutor();

    /**
     * @brief Initialize NebdMetaFileManager
     * @return nullptr - initialization failed; otherwise, it indicates successful initialization
     */
    MetaFileManagerPtr InitMetaFileManager();

    /**
     * @brief Initialize HeartbeatManagerOption
     * @param[out] opt
     * @return false - initialization failed true - initialization successful
     */
    bool InitHeartbeatManagerOption(HeartbeatManagerOption *opt);

    /**
     * @brief Initialize HeartbeatManager
     * @return false - initialization failed true - initialization successful
     */
    bool InitHeartbeatManager();

    /**
     * @brief Start brpc service
     * @return false - Failed to start service true - Successfully started service
     */
    bool StartServer();

 private:
    //Configuration Item
    Configuration conf_;
    //NebdServer Listening Address
    std::string listenAddress_;
    //Is NebdServer in running state
    bool isRunning_ =  false;

    // brpc server
    brpc::Server server_;
    //Used to accept and process various requests from the client side
    std::shared_ptr<NebdFileManager> fileManager_;
    //Responsible for handling file heartbeat timeout
    std::shared_ptr<HeartbeatManager> heartbeatManager_;
    // curveclient
    std::shared_ptr<CurveClient> curveClient_;
};

}  // namespace server
}  // namespace nebd

#endif  // NEBD_SRC_PART2_NEBD_SERVER_H_

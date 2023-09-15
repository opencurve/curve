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
 * Created Date: 2020-02-18
 * Author: charisu
 */

#ifndef SRC_TOOLS_VERSION_TOOL_H_
#define SRC_TOOLS_VERSION_TOOL_H_

#include <string>
#include <map>
#include <vector>
#include <memory>
#include "src/tools/mds_client.h"
#include "src/tools/metric_client.h"
#include "src/common/string_util.h"
#include "src/tools/snapshot_clone_client.h"

namespace curve {
namespace tool {

using VersionMapType = std::map<std::string, std::vector<std::string>>;
using ProcessMapType = std::map<std::string, std::vector<std::string>>;
using ClientVersionMapType = std::map<std::string, VersionMapType>;
const char kOldVersion[] = "before-0.0.5.2";
const char kProcessNebdServer[] = "nebd-server";
const char kProcessQemu[] = "qemu";
const char kProcessPython[] = "python";
const char kProcessOther[] = "other";

class VersionTool {
 public:
    explicit VersionTool(std::shared_ptr<MDSClient> mdsClient,
                         std::shared_ptr<MetricClient> metricClient,
                         std::shared_ptr<SnapshotCloneClient> snapshotClient)
        : mdsClient_(mdsClient), snapshotClient_(snapshotClient),
          metricClient_(metricClient) {}
    virtual ~VersionTool() {}

    /**
     * @brief Get the version of mds and check version consistency
     * @param[out] version version
     * @return returns 0 for success, -1 for failure
     */
    virtual int GetAndCheckMdsVersion(std::string *version,
                                      std::vector<std::string> *failedList);

    /**
     * @brief Get the version of chunkserver and check version consistency
     * @param[out] version version
     * @return returns 0 for success, -1 for failure
     */
    virtual int
    GetAndCheckChunkServerVersion(std::string *version,
                                  std::vector<std::string> *failedList);

    /**
     * @brief Get the version of the snapshot clone server
     * @param[out] version version
     * @return returns 0 for success, -1 for failure
     */
    virtual int
    GetAndCheckSnapshotCloneVersion(std::string *version,
                                    std::vector<std::string> *failedList);

    /**
     * @brief Get the version of the client
     * @param[out] versionMap process ->Version ->Address mapping table
     * @return returns 0 for success, -1 for failure
     */
    virtual int GetClientVersion(ClientVersionMapType *versionMap);

    /**
     * @brief Print the address corresponding to each version
     * @param versionMap version to address list map
     */
    static void PrintVersionMap(const VersionMapType &versionMap);

    /**
     * @brief Print access failed addresses
     * @param failedList Access Failed Address List
     */
    static void PrintFailedList(const std::vector<std::string> &failedList);

 private:
    /**
     * @brief Obtain the version of the address corresponding to addrVec and store the version and address correspondence in the map
     * @param addrVec Address List
     * @param[out] versionMap version to address map
     * @param[out] failedList Query address list for version failure
     */
    void GetVersionMap(const std::vector<std::string> &addrVec,
                       VersionMapType *versionMap,
                       std::vector<std::string> *failedList);

    /**
     * @brief Obtain the version of the address corresponding to addrVec and store the version and address correspondence in the map
     * @param addrVec Address List
     * @param[out] processMap The address list of clients corresponding to different processes
     */
    void FetchClientProcessMap(const std::vector<std::string> &addrVec,
                               ProcessMapType *processMap);

    /**
     * @brief Get the name of the corresponding program from the command line of starting the server
     *         For example, the command behavior of nebd
     *         process_cmdline : "/usr/bin/nebd-server
     *         -confPath=/etc/nebd/nebd-server.conf
     *         -log_dir=/data/log/nebd/server
     *         -graceful_quit_on_sigterm=true
     *         -stderrthreshold=3
     *         "
     *         So the name we need to resolve is nebd server
     * @param addrVec Address List
     * @return The name of the process
     */
    std::string GetProcessNameFromCmd(const std::string &cmd);

 private:
    // Client sending RPC to mds
    std::shared_ptr<MDSClient> mdsClient_;
    // Used to obtain snapshot clone status
    std::shared_ptr<SnapshotCloneClient> snapshotClient_;
    // Obtain metric clients
    std::shared_ptr<MetricClient> metricClient_;
};

}  // namespace tool
}  // namespace curve
#endif  // SRC_TOOLS_VERSION_TOOL_H_

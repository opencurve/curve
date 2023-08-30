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
 * Created Date: 2019-10-30
 * Author: charisu
 */

#ifndef SRC_TOOLS_COPYSET_CHECK_H_
#define SRC_TOOLS_COPYSET_CHECK_H_

#include <gflags/gflags.h>

#include <string>
#include <iostream>
#include <map>
#include <vector>
#include <algorithm>
#include <set>
#include <memory>
#include <iterator>

#include "src/mds/common/mds_define.h"
#include "src/tools/copyset_check_core.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"

using curve::mds::topology::PoolIdType;
using curve::mds::topology::CopySetIdType;
using curve::mds::topology::ChunkServerIdType;
using curve::mds::topology::ServerIdType;

namespace curve {
namespace tool {

class CopysetCheck : public CurveTool {
 public:
    explicit CopysetCheck(std::shared_ptr<CopysetCheckCore> core) :
                        core_(core), inited_(false) {}
    ~CopysetCheck() = default;

    /**
     * @brief Check the health status of the replication group based on the flag
     *   The standard for replication group health is that no replica is in the following state. The following order is sorted by priority,
     *   If the above one is met, the following one will not be checked
     *   1. The leader is empty (the information of the replication group is based on the leader, and cannot be checked without a leader)
     *   2. Insufficient number of replicas in the configuration
     *   3. Some replicas are not online
     *   4. There is a replica in the installation snapshot
     *   5. The log index difference between replicas is too large
     *   6. For a cluster, it is also necessary to determine whether the number of copysets and the number of leaders on the chunkserver are balanced,
     *        Avoid scheduling that may cause instability in the cluster in the future
     * @param command The command to be executed by currently includes check copyset, check chunkserver,
     *                  Check server, check cluster, etc
     * @return returns 0 for success, -1 for failure
     */
    int RunCommand(const std::string& command) override;

    /**
     * @brief Print help information
     * @param command The command to be executed by currently includes check copyset, check chunkserver,
     *            Check server, check cluster, etc
     */
    void PrintHelp(const std::string& command) override;

    /**
     * @brief returns whether the command is supported
     * @param command: The command executed
     * @return true/false
     */
    static bool SupportCommand(const std::string& command);

 private:
   /**
     * @brief initialization
     */
    int Init();

    /**
     * @brief Check a single copyset
     * @return Health returns 0, otherwise returns -1
     */
    int CheckCopyset();

    /**
     * @brief Check all copysets on chunkserver
     * @return Health returns 0, otherwise returns -1
     */
    int CheckChunkServer();

    /**
     * @brief Check all copysets on the server
     * @return Health returns 0, otherwise returns -1
     */
    int CheckServer();

    /**
     * @brief Check all copysets in the cluster
     * @return Health returns 0, otherwise returns -1
     */
    int CheckCopysetsInCluster();

    /**
     * @brief Check the operator on the mds side
     * @return returns 0 without an operator, otherwise returns -1
     */
    int CheckOperator(const std::string& opName);

    //Print detailed results of copyset check
    void PrintDetail();
    void PrintCopySet(const std::set<std::string>& set);

    //Print the results of the inspection, how many copies are there in total, and how many are unhealthy
    void PrintStatistic();

    //Print a list of problematic chunkservers
    void PrintExcepChunkservers();

    //Print the volume on most offline copies
    int PrintMayBrokenVolumes();

 private:
    //Check the core logic of copyset
    std::shared_ptr<CopysetCheckCore> core_;
    //Has initialization been successful
    bool inited_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_COPYSET_CHECK_H_

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
 * File Created: Saturday, 29th June 2019 12:35:00 pm
 * Author: tongguangxun
 */

#ifndef SRC_TOOLS_CONSISTENCY_CHECK_H_
#define SRC_TOOLS_CONSISTENCY_CHECK_H_

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <vector>
#include <string>
#include <iostream>
#include <memory>
#include <set>
#include <utility>
#include <map>

#include "proto/copyset.pb.h"
#include "src/common/net_common.h"
#include "src/tools/namespace_tool_core.h"
#include "src/tools/chunkserver_client.h"
#include "src/tools/curve_tool.h"
#include "src/tools/curve_tool_define.h"

DECLARE_string(filename);
DECLARE_bool(check_hash);

namespace curve {
namespace tool {
using CopySet = std::pair<PoolIdType, CopySetIdType>;
using CsAddrsType = std::vector<std::string>;

std::ostream& operator<<(std::ostream& os, const CopySet& copyset);
std::ostream& operator<<(std::ostream& os, const CsAddrsType& csAddrs);

class ConsistencyCheck : public CurveTool {
 public:
    ConsistencyCheck(std::shared_ptr<NameSpaceToolCore> nameSpaceToolCore,
                         std::shared_ptr<ChunkServerClient> csClient);
    ~ConsistencyCheck() = default;

    /**
     * @brief Print help information
     * @param cmd: Command executed
     * @return None
     */
    void PrintHelp(const std::string &cmd) override;

    /**
     * @brief Execute command
     * @param cmd: Command executed
     * @return returns 0 for success, -1 for failure
     */
    int RunCommand(const std::string &cmd) override;

    /**
     * @brief Check consistency of three replicas
     * @param fileName The file name to check for consistency
     * @param checkHash Does check hash? If false, check apply index instead of hash
     * @return consistently returns 0, otherwise returns -1
     */
    int CheckFileConsistency(const std::string& fileName, bool checkHash);

    /**
     * @brief Check the consistency of the three copies of the copyset
     * @param copysetId The copysetId to be checked
     * @param checkHash Does check hash? If false, check apply index instead of hash
     * @return returns 0 for success, -1 for failure
     */
    int CheckCopysetConsistency(const CopySet copysetId,
                                bool checkHash);

    /**
     * @brief Print help information
     */
    void PrintHelp();

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
     * @brief Get the list of copysets where the file is located from mds
     * @param fileName File name
     * @param[out] copysetIds  The list copysetIds is valid when the return value is 0
     * @return returns 0 for success, -1 for failure
     */
    int FetchFileCopyset(const std::string& fileName,
                         std::set<CopySet>* copysets);

    /**
     * @brief Get the status of copyset from chunkserver
     * @param csAddr chunkserver address
     * @param copysetId The copysetId to obtain
     * @param[out] response The response returned 
     * @return returns 0 for success, -1 for failure
     */
    int GetCopysetStatusResponse(const std::string& csAddr,
                                 const CopySet copyset,
                                 CopysetStatusResponse* response);

    /**
     * @brief Check the consistency of the hash of the specified chunk in the copyset
     * @param copysetId The copysetId to be checked
     * @param csAddrs The address of the chunkserver corresponding to the copyset
     * @return consistently returns 0, otherwise returns -1
     */
    int CheckCopysetHash(const CopySet& copyset,
                         const CsAddrsType& csAddrs);

    /**
     * @brief Consistency of hash in three replicates of chunk
     * @param chunk The chunk to be checked
     * @param csAddrs The address of the chunkserver corresponding to the copyset
     * @return consistently returns 0, otherwise returns -1
     */
    int CheckChunkHash(const Chunk& chunk,
                       const CsAddrsType& csAddrs);

    /**
     * @brief Check the consistency of the applyindex between replicas
     * @param copysetId The copysetId to be checked
     * @param csAddrs The address of the chunkserver corresponding to the copyset
     * @return consistently returns 0, otherwise returns -1
     */
    int CheckApplyIndex(const CopySet copyset,
                        const CsAddrsType& csAddrs);

 private:
    // The logical pool ID where the file is located
    PoolIdType  lpid_;
    // Used to interact with the nameservice interface of mds
    std::shared_ptr<NameSpaceToolCore> nameSpaceToolCore_;
    // Client sending RPC to chunkserver
    std::shared_ptr<ChunkServerClient> csClient_;
    // The chunk of the hash needs to be checked in the copyset
    std::map<CopySet, std::set<uint64_t>> chunksInCopyset_;
    // Has initialization been successful
    bool inited_;
};
}  // namespace tool
}  // namespace curve
#endif  // SRC_TOOLS_CONSISTENCY_CHECK_H_

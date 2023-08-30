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
 * Date: Sat Sep  5 11:13:12 CST 2020
 */

#ifndef SRC_CLIENT_UNSTABLE_HELPER_H_
#define SRC_CLIENT_UNSTABLE_HELPER_H_

#include <bthread/mutex.h>

#include <string>
#include <unordered_map>
#include <unordered_set>

#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/service_helper.h"

namespace curve {
namespace client {

enum class UnstableState {
    NoUnstable,
    ChunkServerUnstable,
    ServerUnstable
};

//If the chunkserver goes down or the network is unreachable, the rpc sent to the corresponding chunkserver will time out
//After returning, go back to the refresh leader and then send the request
//In this case, requests on different copysets will always first rpc timeout and then refresh the leader again
//To avoid a redundant rpc timeout
//Record the number of timeout requests sent to the same chunkserver
//If the threshold is exceeded, an HTTP request will be sent to check if the chunkserver is healthy
//If not healthy, notify all leaders of the copyset on this chunkserver
//Actively refresh the leader instead of directly sending rpc based on cached leader information
class UnstableHelper {
 public:
    UnstableHelper() = default;

    UnstableHelper(const UnstableHelper&) = delete;
    UnstableHelper& operator=(const UnstableHelper&) = delete;

    void Init(const ChunkServerUnstableOption& opt) {
        option_ = opt;
    }

    void IncreTimeout(ChunkServerID csId) {
        std::unique_lock<decltype(mtx_)> guard(mtx_);
        ++timeoutTimes_[csId];
    }

    UnstableState GetCurrentUnstableState(ChunkServerID csId,
                                          const butil::EndPoint& csEndPoint);

    void ClearTimeout(ChunkServerID csId, const butil::EndPoint& csEndPoint) {
        std::string ip = butil::ip2str(csEndPoint.ip).c_str();

        std::unique_lock<decltype(mtx_)> guard(mtx_);
        timeoutTimes_[csId] = 0;
        serverUnstabledChunkservers_[ip].clear();
    }

 private:
    /**
     * @brief Check chunkserver status
     *
     * @param: endPoint, IP:port address of endPoint chunkserver
     * @return: true healthy/false unhealthy
     */
    bool CheckChunkServerHealth(const butil::EndPoint& endPoint) const {
        return ServiceHelper::CheckChunkServerHealth(
                   endPoint, option_.checkHealthTimeoutMS) == 0;
    }

    ChunkServerUnstableOption option_;

    bthread::Mutex mtx_;

    //Number of consecutive timeout requests for the same chunkserver
    std::unordered_map<ChunkServerID, uint32_t> timeoutTimes_;

    //The ID of an unstable chunkserver on the same server
    std::unordered_map<std::string, std::unordered_set<ChunkServerID>>
        serverUnstabledChunkservers_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_UNSTABLE_HELPER_H_

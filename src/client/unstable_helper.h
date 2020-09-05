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

// 如果chunkserver宕机或者网络不可达, 发往对应chunkserver的rpc会超时
// 返回之后, 回去refresh leader然后再去发送请求
// 这种情况下不同copyset上的请求，总会先rpc timedout然后重新refresh leader
// 为了避免一次多余的rpc timedout
// 记录一下发往同一个chunkserver上超时请求的次数
// 如果超过一定的阈值，会发送http请求检查chunkserver是否健康
// 如果不健康，则通知所有leader在这台chunkserver上的copyset
// 主动去refresh leader，而不是根据缓存的leader信息直接发送rpc
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
     * @brief 检查chunkserver状态
     *
     * @param: endPoint chunkserver的ip:port地址
     * @return: true 健康 / false 不健康
     */
    bool CheckChunkServerHealth(const butil::EndPoint& endPoint) const {
        return ServiceHelper::CheckChunkServerHealth(
                   endPoint, option_.checkHealthTimeoutMS) == 0;
    }

    ChunkServerUnstableOption option_;

    bthread::Mutex mtx_;

    // 同一chunkserver连续超时请求次数
    std::unordered_map<ChunkServerID, uint32_t> timeoutTimes_;

    // 同一server上unstable chunkserver的id
    std::unordered_map<std::string, std::unordered_set<ChunkServerID>>
        serverUnstabledChunkservers_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_UNSTABLE_HELPER_H_

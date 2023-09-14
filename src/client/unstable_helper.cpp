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

#include "src/client/unstable_helper.h"

namespace curve {
namespace client {

UnstableState
UnstableHelper::GetCurrentUnstableState(ChunkServerID csId,
                                        const butil::EndPoint &csEndPoint) {
    std::string ip = butil::ip2str(csEndPoint.ip).c_str();

    mtx_.lock();
    // If the current IP has exceeded the threshold, it will directly return chunkserver unstable
    uint32_t unstabled = serverUnstabledChunkservers_[ip].size();
    if (unstabled >= option_.serverUnstableThreshold) {
        serverUnstabledChunkservers_[ip].emplace(csId);
        mtx_.unlock();
        return UnstableState::ChunkServerUnstable;
    }

    bool exceed =
        timeoutTimes_[csId] > option_.maxStableChunkServerTimeoutTimes;
    mtx_.unlock();

    if (exceed == false) {
        return UnstableState::NoUnstable;
    }

    bool health = CheckChunkServerHealth(csEndPoint);
    if (health) {
        ClearTimeout(csId, csEndPoint);
        return UnstableState::NoUnstable;
    }

    mtx_.lock();
    auto ret = serverUnstabledChunkservers_[ip].emplace(csId);
    unstabled = serverUnstabledChunkservers_[ip].size();
    mtx_.unlock();

    if (ret.second && unstabled == option_.serverUnstableThreshold) {
        return UnstableState::ServerUnstable;
    } else {
        return UnstableState::ChunkServerUnstable;
    }
}

}  // namespace client
}  // namespace curve

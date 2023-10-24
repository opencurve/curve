/*
 *  Copyright (c) 2023 NetEase Inc.
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

#ifndef CURVEFS_SRC_CLIENT_RPCCLIENT_FSDELTA_UPDATER_H_
#define CURVEFS_SRC_CLIENT_RPCCLIENT_FSDELTA_UPDATER_H_

#include <atomic>

namespace curvefs {
namespace client {

class FsDeltaUpdater {
 public:
    static FsDeltaUpdater& GetInstance() {
        static FsDeltaUpdater instance_;
        return instance_;
    }

    void Init() { deltaBytes_.store(0); }

    void UpdateDeltaBytes(int64_t deltaBytes);

    int64_t GetDeltaBytesAndReset();

    int64_t GetDeltaBytes();

 private:
    uint32_t fsId_;
    std::atomic<int64_t> deltaBytes_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_FSDELTA_UPDATER_H_

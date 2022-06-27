/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * File Created: 2022-06-30
 * Author: xuchaojie
 */

#include "src/client/chunkserver_broadcaster.h"

#include "src/common/task_tracker.h"

using ::curve::common::TaskTracker;

namespace curve {
namespace client {

class UpdateFileEpochClosure : public ChunkServerClientClosure {
 public:
    UpdateFileEpochClosure() {}
    virtual ~UpdateFileEpochClosure() {}
    void Run() override {
        std::unique_ptr<UpdateFileEpochClosure> self_guard(this);
        tracker_->HandleResponse(GetErrorCode());
    }

    void AddToBeTraced(const std::shared_ptr<TaskTracker> &tracker) {
        tracker->AddOneTrace();
        tracker_ = tracker;
    }

 private:
    std::shared_ptr<TaskTracker> tracker_;
};

int ChunkServerBroadCaster::BroadCastFileEpoch(
    uint64_t fileId, uint64_t epoch,
    const std::list<CopysetPeerInfo<ChunkServerID>> &csLocs) {
    int ret = LIBCURVE_ERROR::OK;
    auto tracker = std::make_shared<TaskTracker>();
    for (const auto &cs : csLocs) {
        UpdateFileEpochClosure *done = new UpdateFileEpochClosure();
        done->AddToBeTraced(tracker);
        ret = csClient_->UpdateFileEpoch(cs, fileId, epoch, done);
        if (ret != LIBCURVE_ERROR::OK) {
            // already failed, wait all inflight rpc to be done
            tracker->Wait();
            LOG(ERROR) << "BroadCastFileEpoch request failed, ret: " << ret
                       << ", chunkserverid: " << cs.peerID
                       << ", fileId: " << fileId
                       << ", epoch: " << epoch;
            return ret;
        }
        if (tracker->GetTaskNum() >= option_.broadCastMaxNum) {
            tracker->WaitSome(1);
        }
        ret = tracker->GetResult();
        if (ret != LIBCURVE_ERROR::OK) {
            // already failed, wait all inflight rpc to be done
            tracker->Wait();
            LOG(ERROR) << "BroadCastFileEpoch found some request failed, ret: "
                       << ret;
            return ret;
        }
    }
    tracker->Wait();
    ret = tracker->GetResult();
    if (ret != LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "BroadCastFileEpoch found some request failed, ret: "
                   << ret;
        return ret;
    }

    return LIBCURVE_ERROR::OK;
}


}   // namespace client
}   // namespace curve

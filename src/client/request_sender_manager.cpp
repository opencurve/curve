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
 * Created Date: 18-10-9
 * Author: wudemiao
 */

#include "src/client/request_sender_manager.h"

#include <utility>

#include "src/client/request_sender.h"

namespace curve {
namespace client {

RequestSenderManager::SenderPtr RequestSenderManager::GetOrCreateSender(
    const ChunkServerID& leaderId,
    const butil::EndPoint& leaderAddr,
    const IOSenderOption& senderopt) {
    {
        curve::common::ReadLockGuard guard(rwlock_);
        auto iter = senderPool_.find(leaderId);
        if (senderPool_.end() != iter) {
            return iter->second;
        }
    }

    curve::common::WriteLockGuard guard(rwlock_);
    auto iter = senderPool_.find(leaderId);
    if (senderPool_.end() != iter) {
        return iter->second;
    }

    SenderPtr sender = std::make_shared<RequestSender>(leaderId, leaderAddr);
    int rc = sender->Init(senderopt);
    if (rc != 0) {
        return nullptr;
    }

    senderPool_.emplace(leaderId, sender);

    return sender;
}

void RequestSenderManager::ResetSenderIfNotHealth(const ChunkServerID& csId) {
    curve::common::WriteLockGuard guard(rwlock_);
    auto iter = senderPool_.find(csId);

    if (iter == senderPool_.end()) {
        return;
    }

    // 检查是否健康
    if (iter->second->IsSocketHealth()) {
        return;
    }

    senderPool_.erase(iter);
}

}   // namespace client
}   // namespace curve

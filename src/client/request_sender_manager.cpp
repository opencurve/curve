/*
 * Project: curve
 * Created Date: 18-10-9
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#include "src/client/request_sender_manager.h"

#include <utility>

#include "src/client/request_sender.h"

namespace curve {
namespace client {

RequestSenderManager::SenderPtr RequestSenderManager::GetOrCreateSender(
                                            const ChunkServerID &leaderId,
                                            const butil::EndPoint &leaderAddr,
                                            IOSenderOption_t senderopt) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;

    std::lock_guard<std::mutex> guard(lock_);
    auto leaderIter = senderPool_.find(leaderId);
    if (senderPool_.end() != leaderIter) {
        return leaderIter->second;
    } else {
        // 不存在则创建
        senderPtr = std::make_shared<RequestSender>(leaderId, leaderAddr);
        CHECK(nullptr != senderPtr) << "new RequestSender failed";

        int rc = senderPtr->Init(senderopt);
        if (0 != rc) {
            return nullptr;
        }

        senderPool_.insert(std::pair<ChunkServerID, SenderPtr>(leaderId,
                                                               senderPtr));
    }
    return senderPtr;
}

void RequestSenderManager::ResetSenderIfNotHealth(const ChunkServerID& csId) {
    std::lock_guard<std::mutex> guard(lock_);
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

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

RequestSenderManager::SenderPtr RequestSenderManager::CreateOrResetSender(
    const ChunkServerID &leaderId,
    const butil::EndPoint &leaderAddr) {
    std::lock_guard<std::mutex> guard(lock_);
    std::shared_ptr<RequestSender> senderPtr = nullptr;

    auto senderIter = senderPool_.find(leaderId);
    if (senderPool_.end() != senderIter) {
        /* 已经存在 reset */
        senderPtr = senderIter->second;
        if (0 != senderPtr->ResetSender(leaderId, leaderAddr)) {
            return nullptr;
        }
    } else {
        /* 不存在则创建 */
        senderPtr = std::make_shared<RequestSender>(leaderId, leaderAddr);
        CHECK(nullptr != senderPtr) << "new RequestSender failed";
    }

    if (0 != senderPtr->Init()) {
        return nullptr;
    }
    senderPool_.insert(std::pair<ChunkServerID, SenderPtr>(leaderId,
                                                           senderPtr));
    return senderPtr;
}

RequestSenderManager::SenderPtr RequestSenderManager::GetOrCreateSender(
    const ChunkServerID &leaderId, const butil::EndPoint &leaderAddr) {
    std::shared_ptr<RequestSender> senderPtr = nullptr;

    std::lock_guard<std::mutex> guard(lock_);
    auto leaderIter = senderPool_.find(leaderId);
    if (senderPool_.end() != leaderIter) {
        return leaderIter->second;
    } else {
        /* 不存在则创建 */
        senderPtr = std::make_shared<RequestSender>(leaderId, leaderAddr);
        CHECK(nullptr != senderPtr) << "new RequestSender failed";
        if (0 != senderPtr->Init()) {
            return nullptr;
        }
        senderPool_.insert(std::pair<ChunkServerID, SenderPtr>(leaderId,
                                                               senderPtr));
    }
    return senderPtr;
}

}   // namespace client
}   // namespace curve

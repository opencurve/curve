/*
 * Project: curve
 * Created Date: 18-10-9
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CLIENT_CHUNK_REQUEST_SENDER_MANAGER_H
#define CURVE_CLIENT_CHUNK_REQUEST_SENDER_MANAGER_H

#include <mutex>    //NOLINT
#include <unordered_map>
#include <memory>

#include "src/client/client_common.h"
#include "src/common/uncopyable.h"

namespace curve {
namespace client {

using curve::common::Uncopyable;

class RequestSender;

class RequestSenderManager : public Uncopyable{
 public:
    using SenderPtr = std::shared_ptr<RequestSender>;

    SenderPtr CreateOrResetSender(const ChunkServerID &leaderId,
                                  const butil::EndPoint &leaderAddr);
    SenderPtr GetOrCreateSender(const ChunkServerID &leaderId,
                        const butil::EndPoint &leaderAddr);

 private:
    mutable std::mutex lock_;
    std::unordered_map<ChunkServerID, SenderPtr> senderPool_;
};

}   // namespace client
}   // namespace curve

#endif  // CURVE_CLIENT_CHUNK_REQUEST_SENDER_MANAGER_H

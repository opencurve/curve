/*
 * Project: curve
 * Created Date: 18-10-9
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_CLIENT_REQUEST_SENDER_MANAGER_H_
#define SRC_CLIENT_REQUEST_SENDER_MANAGER_H_

#include <mutex>    //NOLINT
#include <unordered_map>
#include <memory>

#include "src/client/client_common.h"
#include "src/client/config_info.h"
#include "src/common/uncopyable.h"

namespace curve {
namespace client {

using curve::common::Uncopyable;

class RequestSender;
/**
 * 所有Chunk Server的request sender管理者，
 * 可以理解为Chunk Server的链接管理者
 */
class RequestSenderManager : public Uncopyable {
 public:
    using SenderPtr = std::shared_ptr<RequestSender>;
    RequestSenderManager() : lock_(), senderPool_() {}

    /**
     * 获取指定leader id的sender，如果没有则根据leader
     * 地址，创建新的 sender并返回
     * @param leaderId:leader的id
     * @param leaderAddr:leader的地址
     * @return nullptr:get或者create失败，否则成功
     */
    SenderPtr GetOrCreateSender(const ChunkServerID &leaderId,
                                const butil::EndPoint &leaderAddr,
                                IOSenderOption_t senderopt);

 private:
    // 互斥锁，保护senderPool_
    mutable std::mutex lock_;
    // 请求发送链接的map，以ChunkServer ID为key
    std::unordered_map<ChunkServerID, SenderPtr> senderPool_;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_REQUEST_SENDER_MANAGER_H_

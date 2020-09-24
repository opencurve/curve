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

#ifndef SRC_CLIENT_REQUEST_SENDER_MANAGER_H_
#define SRC_CLIENT_REQUEST_SENDER_MANAGER_H_

#include <memory>
#include <unordered_map>

#include "src/client/client_common.h"
#include "src/client/config_info.h"
#include "src/common/concurrent/rw_lock.h"
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
    RequestSenderManager() : rwlock_(), senderPool_() {}

    /**
     * 获取指定leader id的sender，如果没有则根据leader
     * 地址，创建新的 sender并返回
     * @param leaderId:leader的id
     * @param leaderAddr:leader的地址
     * @return nullptr:get或者create失败，否则成功
     */
    SenderPtr GetOrCreateSender(const ChunkServerID& leaderId,
                                const butil::EndPoint& leaderAddr,
                                const IOSenderOption& senderopt);

    /**
     * @brief 如果csId对应的RequestSender不健康，就进行重置
     * @param csId chunkserver id
     */
    void ResetSenderIfNotHealth(const ChunkServerID& csId);

 private:
    // 读写锁，保护senderPool_
    curve::common::BthreadRWLock rwlock_;
    // 请求发送链接的map，以ChunkServer ID为key
    std::unordered_map<ChunkServerID, SenderPtr> senderPool_;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_REQUEST_SENDER_MANAGER_H_

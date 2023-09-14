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
 * Request sender managers for all Chunk Servers,
 * It can be understood as the link manager of Chunk Server
 */
class RequestSenderManager : public Uncopyable {
 public:
    using SenderPtr = std::shared_ptr<RequestSender>;
    RequestSenderManager() : rwlock_(), senderPool_() {}

    /**
     * Obtain the sender with the specified leader ID, if not, based on the leader
     * Address, create a new sender and return
     * @param leaderId: The ID of the leader
     * @param leaderAddr: The address of the leader
     * @return nullptr: Get or create failed, otherwise successful
     */
    SenderPtr GetOrCreateSender(const ChunkServerID& leaderId,
                                const butil::EndPoint& leaderAddr,
                                const IOSenderOption& senderopt);

    /**
     * @brief If the RequestSender corresponding to csId is not healthy, reset it
     * @param csId chunkserver id
     */
    void ResetSenderIfNotHealth(const ChunkServerID& csId);

 private:
    // Read write lock to protect senderPool_
    curve::common::BthreadRWLock rwlock_;
    // Request to send a map for the link, with ChunkServer ID as the key
    std::unordered_map<ChunkServerID, SenderPtr> senderPool_;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_REQUEST_SENDER_MANAGER_H_

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

/*
 * Project: curve
 * Created Date: 2023-06-09
 * Author: wanghai (SeanHai)
*/

#ifndef SRC_CLIENT_AUTH_CLIENT_H_
#define SRC_CLIENT_AUTH_CLIENT_H_

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <memory>
#include "proto/auth.pb.h"
#include "src/client/rpc_excutor.h"
#include "src/common/authenticator.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/interruptible_sleeper.h"
#include "src/common/concurrent/name_lock.h"
#include "src/mds/auth/authnode.h"

using ::curve::mds::auth::Token;
using ::curve::mds::auth::Ticket;
using ::curve::mds::auth::TicketAttach;
using ::curve::common::AuthClientOption;
using ::curve::common::RWLock;
using ::curve::common::Thread;
using ::curve::common::InterruptibleSleeper;

namespace curve {
namespace client {

class AuthClient {
 public:
    static AuthClient& GetInstance() {
        static AuthClient instance;
        return instance;
    }

    ~AuthClient() { Uninit(); }

    void Init(const MetaServerOption &rpcOption,
      const AuthClientOption &authOption,
      std::shared_ptr<curve::mds::auth::AuthNode> authNode = nullptr);

    void Uninit();

    bool GetToken(const std::string &serverId, Token *token);

    bool Enable() const;

    int MapSize() const;

 private:
    AuthClient() {
      isStop_.store(true);
      isInit_.store(false);
    }
    AuthClient(const AuthClient&) = delete;
    AuthClient& operator=(const AuthClient&) = delete;

    bool BuildToken(const std::pair<TicketAttach, std::string> &ticketInfo,
                    Token *token);

    bool RefreshTicket(std::string serverId);

    void Refresh();

    bool DecTicketAttach(const std::string &encTicketAttach,
        TicketAttach *info);

 private:
    curve::common::Atomic<bool> isInit_;
    MetaServerOption rpcOpt_;
    AuthClientOption authOpt_;
    // for get ticket form remote
    RPCExcutorRetryPolicy rpcExcutor_;
    // for get ticket from local
    std::shared_ptr<curve::mds::auth::AuthNode> authNode_;
    // map<serverId, <expiration, ticket>>
    std::unordered_map<std::string,
        std::pair<TicketAttach, std::string>> ticketMap_;
    mutable RWLock ticketMapMutex_;
    curve::common::NameLock nameLock_;

    curve::common::Thread refreshThread_;
    curve::common::Atomic<bool> isStop_;
    InterruptibleSleeper sleeper_;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_AUTH_CLIENT_H_
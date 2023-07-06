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
 * Created Date: 2023-05-24
 * Author: wanghai (SeanHai)
 */

#ifndef SRC_MDS_AUTH_AUTHNODE_H_
#define SRC_MDS_AUTH_AUTHNODE_H_

#include <cstdint>
#include <string>
#include <memory>
#include <unordered_map>
#include "proto/auth.pb.h"
#include "src/mds/auth/auth_storage_etcd.h"
#include "src/common/concurrent/rw_lock.h"
#include "src/common/authenticator.h"


namespace curve {
namespace mds {
namespace auth {

using ::curve::common::RWLock;
using ::curve::common::ReadLockGuard;
using ::curve::common::WriteLockGuard;

struct AuthOption {
    std::string authKey;
    uint32_t ticketTimeoutSec;
    AuthOption() : ticketTimeoutSec(1800) {}
};

class AuthNode {
 public:
    AuthNode() = default;
    virtual ~AuthNode() = default;

    virtual AuthStatusCode AddKey(const std::string &encKey) = 0;
    virtual AuthStatusCode DeleteKey(const std::string &encKeyId) = 0;
    virtual AuthStatusCode UpdateKey(const std::string &encKey) = 0;
    virtual AuthStatusCode GetKey(const std::string &encKeyId,
        std::string *encKey) = 0;
    virtual AuthStatusCode GetTicket(const std::string &cId,
        const std::string &sId, std::string *encTicket, std::string *encSK) = 0;
};

class AuthNodeImpl : public AuthNode {
 public:
    explicit AuthNodeImpl(std::shared_ptr<AuthStorage> storage)
        : storage_(storage) {}

    bool Init(const AuthOption &option);
    AuthStatusCode AddKey(const std::string &encKey) override;
    AuthStatusCode DeleteKey(const std::string &encKeyId) override;
    AuthStatusCode UpdateKey(const std::string &encKey) override;
    AuthStatusCode GetKey(const std::string &encKeyId,
        std::string *encKey) override;
    AuthStatusCode GetTicket(const std::string &cId,
        const std::string &sId, std::string *encTicket,
        std::string *encAttach) override;

 private:
    AuthStatusCode EncryptByAuthKey(const std::string &in, std::string *out);
    AuthStatusCode DecryptByAuthKey(const std::string &in, std::string *out);

 private:
    std::unordered_map<std::string, Key> keyMap_;
    mutable RWLock keyMapMutex_;
    uint32_t ticketTimeoutSec_;
    std::shared_ptr<AuthStorage> storage_;
};

}  // namespace auth
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_AUTH_AUTHNODE_H_

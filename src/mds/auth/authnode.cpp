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
 * Created Date: 2023-05-25
 * Author: wanghai (SeanHai)
 */

#include "src/mds/auth/authnode.h"
#include "proto/auth.pb.h"
#include "src/common/authenticator.h"
#include "src/common/uuid.h"
#include "src/common/timeutility.h"

namespace curve {
namespace mds {
namespace auth {

using ::curve::common::UUIDGenerator;
using ::curve::common::Encryptor;
using ::curve::common::TimeUtility;
using ::curve::common::ZEROIV;

bool AuthNodeImpl::Init(const AuthOption &option) {
    ticketTimeoutSec_ = option.ticketTimeoutSec;
    WriteLockGuard keyMapLG(keyMapMutex_);
    if (!storage_->LoadKeys(&keyMap_)) {
        LOG(ERROR) << "AuthNodeImpl init, load auth keys faild.";
        return false;
    }
    // first time, need init auth key
    auto it = keyMap_.find(curve::common::AUTH_ROLE);
    if (it == keyMap_.end()) {
        Key key;
        key.set_id(curve::common::AUTH_ROLE);
        key.set_key(option.authKey);
        key.set_caps("*");
        key.set_role(RoleType::ROLE_SERVICE);
        keyMap_[curve::common::AUTH_ROLE] = key;
        if (!storage_->StorageKey(key)) {
            LOG(ERROR) << "AuthNodeImpl init, storage auth key faild.";
            return false;
        }
    }
    return true;
}

AuthStatusCode AuthNodeImpl::DecryptByAuthKey(
    const std::string &in, std::string *out) {
    std::string authKey;
    {
        ReadLockGuard keyMapLG(keyMapMutex_);
        auto it = keyMap_.find(curve::common::AUTH_ROLE);
        if (it == keyMap_.end()) {
            LOG(ERROR) << "DecryptByAuthKey failed, the auth key not found.";
            return AuthStatusCode::AUTH_KEY_NOT_EXIST;
        }
        authKey = it->second.key();
    }
    auto ret = Encryptor::AESDecrypt(authKey, ZEROIV, in, out);
    if (ret != 0) {
        LOG(ERROR) << "DecryptByAuthKey failed, in = " << in;
        return AuthStatusCode::AUTH_DECRYPT_FAILED;
    }
    return AuthStatusCode::AUTH_OK;
}

AuthStatusCode AuthNodeImpl::EncryptByAuthKey(
    const std::string &in, std::string *out) {
    std::string authKey;
    {
        ReadLockGuard keyMapLG(keyMapMutex_);
        auto it = keyMap_.find(curve::common::AUTH_ROLE);
        if (it == keyMap_.end()) {
            LOG(ERROR) << "EncryptByAuthKey failed, the auth key not found.";
            return AuthStatusCode::AUTH_KEY_NOT_EXIST;
        }
        authKey = it->second.key();
    }
    auto ret = Encryptor::AESEncrypt(authKey, ZEROIV, in, out);
    if (ret != 0) {
        LOG(ERROR) << "EncryptByAuthKey failed, in = " << in;
        return AuthStatusCode::AUTH_DECRYPT_FAILED;
    }
    return AuthStatusCode::AUTH_OK;
}

AuthStatusCode AuthNodeImpl::DeleteKey(const std::string &encKeyId) {
    std::string decKeyId;
    auto ret = DecryptByAuthKey(encKeyId, &decKeyId);
    if (AuthStatusCode::AUTH_OK != ret) {
        return ret;
    }

    WriteLockGuard keyMapLG(keyMapMutex_);
    LOG(INFO) << "Delete key, key id = " << decKeyId;
    keyMap_.erase(decKeyId);
    if (!storage_->DeleteKey(decKeyId)) {
        return AuthStatusCode::AUTH_DELETE_KEY_FAILED;
    }
    return AuthStatusCode::AUTH_OK;
}

AuthStatusCode AuthNodeImpl::AddOrUpdateKey(const std::string &encKey) {
    std::string decKeyStr;
    auto ret = DecryptByAuthKey(encKey, &decKeyStr);
    if (AuthStatusCode::AUTH_OK != ret) {
        return ret;
    }
    Key key;
    if (!key.ParseFromString(decKeyStr)) {
        LOG(ERROR) << "Update key failed, parse key failed";
        return AuthStatusCode::AUTH_DECRYPT_FAILED;
    }

    WriteLockGuard keyMapLG(keyMapMutex_);
    auto it = keyMap_.find(key.id());
    if (it == keyMap_.end()) {
        LOG(INFO) << "add key = " << key.DebugString();
    } else {
        LOG(INFO) << "update key, oldkey = " << keyMap_[key.id()].DebugString()
                  << ", newkey = " << key.DebugString();
    }
    keyMap_[key.id()] = key;
    if (!storage_->StorageKey(key)) {
        return AuthStatusCode::AUTH_STORE_KEY_FAILED;
    }
    return AuthStatusCode::AUTH_OK;
}

AuthStatusCode AuthNodeImpl::GetKey(const std::string &encKeyId,
    std::string *encKey) {
    std::string decKeyId;
    auto ret = DecryptByAuthKey(encKeyId, &decKeyId);
    if (AuthStatusCode::AUTH_OK != ret) {
        return ret;
    }
    Key key;
    {
        ReadLockGuard keyMapLG(keyMapMutex_);
        auto it = keyMap_.find(decKeyId);
        if (it == keyMap_.end()) {
            LOG(ERROR) << "Get key failed, not found, key id = " << decKeyId;
            return AuthStatusCode::AUTH_KEY_NOT_EXIST;
        }
        key = it->second;
    }
    std::string keyStr;
    if (!key.SerializeToString(&keyStr)) {
        LOG(ERROR) << "Get key failed, serialize key failed, key id = "
                   << decKeyId;
        return AuthStatusCode::AUTH_ENCODE_FAILED;
    }
    return EncryptByAuthKey(keyStr, encKey);
}

AuthStatusCode AuthNodeImpl::GetTicket(const std::string &cId,
    const std::string &sId, std::string *encTicket, std::string *encAttach) {
    ReadLockGuard keyMapLG(keyMapMutex_);
    auto it = keyMap_.find(cId);
    if (it == keyMap_.end()) {
        LOG(ERROR) << "GetTicket failed, cId not found, cId = " << cId;
        return AuthStatusCode::AUTH_KEY_NOT_EXIST;
    }
    it = keyMap_.find(sId);
    if (it == keyMap_.end()) {
        LOG(ERROR) << "GetTicket failed, sId not found, sId = " << sId;
        return AuthStatusCode::AUTH_KEY_NOT_EXIST;
    }

    // 1. generate session key
    UUIDGenerator ug;
    std::string uuid = ug.GenerateUUIDRandom();
    std::string sk = Encryptor::MD516Digest(uuid);

    // 2. generate encrypted ticket
    uint64_t now = TimeUtility::GetTimeofDaySec();
    uint64_t experation = now + ticketTimeoutSec_;
    Ticket ticket;
    ticket.set_cid(cId);
    ticket.set_sid(sId);
    ticket.set_sessionkey(sk);
    ticket.set_expiration(experation);
    ticket.set_caps(keyMap_[cId].caps());
    std::string strTicket;
    if (!ticket.SerializeToString(&strTicket)) {
        LOG(ERROR) << "ticket SerializeToString failed.";
        return AuthStatusCode::AUTH_ENCODE_FAILED;
    }
    int ret = Encryptor::AESEncrypt(keyMap_[sId].key(), ZEROIV,
                                    strTicket, encTicket);
    if (ret != 0) {
        LOG(ERROR) << "encrypted ticket failed, ret = " << ret;
        return AuthStatusCode::AUTH_ENCRYPT_FAILED;
    }

    // 3. generate encrypted ticket attch info
    TicketAttach tAttach;
    tAttach.set_sessionkey(sk);
    tAttach.set_expiration(experation);
    std::string tAttachStr;
    if (!tAttach.SerializeToString(&tAttachStr)) {
        LOG(ERROR) << "ticket attach info SerializeToString failed.";
        return AuthStatusCode::AUTH_ENCODE_FAILED;
    }
    ret = Encryptor::AESEncrypt(keyMap_[cId].key(), ZEROIV,
                                tAttachStr, encAttach);
    if (ret != 0) {
        LOG(ERROR) << "encrypted session key failed, ret = " << ret;
        return AuthStatusCode::AUTH_ENCRYPT_FAILED;
    }
    return AuthStatusCode::AUTH_OK;
}

}  // namespace auth
}  // namespace mds
}  // namespace curve

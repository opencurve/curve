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
 * File Created: Monday, 1st April 2019 5:15:20 pm
 * Author: tongguangxun
 */
#ifndef SRC_COMMON_AUTHENTICATOR_H_
#define SRC_COMMON_AUTHENTICATOR_H_

#include <openssl/aes.h>
#include <glog/logging.h>
#include <cstdint>
#include <cstring>
#include <string>
#include "proto/auth.pb.h"
#include "src/common/timeutility.h"
#include "src/common/configuration.h"

namespace curve {
namespace common {

extern const unsigned char ZEROIV[];
extern const std::string AUTH_ROLE;
extern const std::string MDS_ROLE;
extern const std::string CHUNKSERVER_ROLE;
extern const std::string SNAPSHOTCLONE_ROLE;

class Encryptor {
 public:
    /**
     * bref: 获取要进行签名的字符串
     * @param: date, 当前的时间
     * @param: owner, 文件所有者
     * @return: 返回需要进行加密的字符串
     */
    static std::string GetString2Signature(uint64_t date,
        const std::string& owner);

    /**
    * bref: 为字符串计算签名
    * @param: String2Signature, 需要进行签名计算的字符串
    * @param: secretKey, 为计算的秘钥
    * @return: 返回需要进行签名过后的字符串
    */
    static std::string CalcString2Signature(const std::string& String2Signature,
        const std::string& secretKey);

    static int AESEncrypt(const std::string &key,
        const unsigned char *iv, const std::string &in, std::string *out);

    static int AESDecrypt(const std::string &key,
        const unsigned char *iv, const std::string &in, std::string *out);

    static std::string MD532Digest(const std::string &data);

    static std::string MD516Digest(const std::string &data);

    static bool AESKeyValid(const std::string &key);

    static std::string Base64Encode(const std::string &src);

    static std::string Base64Decode(const std::string &src);

 private:
    static int HMacSha256(const void* key, int key_size,
                          const void* data, int data_size,
                          void* digest);

    static std::string Base64Encode(const unsigned char *src, size_t sz);
};

struct ServerAuthOption {
    bool enable = false;
    std::string key;
    std::string lastKey;
    uint32_t lastKeyTTL = 86400;
    uint32_t requestTTL = 15;
};

struct AuthClientOption {
    bool enable = false;
    std::string clientId;
    std::string key;
    // used for server build-in client key update
    std::string lastKey;
    uint32_t ticketRefreshIntervalSec = 60;
    uint32_t ticketRefreshThresholdSec = 120;
    void Load(Configuration* conf) {
        conf->GetBoolValue("auth.client.enable", &enable);
        if (enable) {
            conf->GetValueFatalIfFail("auth.client.key", &key);
            LOG_IF(FATAL, !curve::common::Encryptor::AESKeyValid(key))
                << "auth.client.key length invalid, key length: "
                << key.length();
            conf->GetValueFatalIfFail("auth.client.id", &clientId);
            conf->GetUInt32Value("auth.client.ticket.refresh.intervalSec",
                &ticketRefreshIntervalSec);
            conf->GetUInt32Value("auth.client.ticket.refresh.threshold",
                &ticketRefreshThresholdSec);
        }
    }
};

class Authenticator {
 public:
    static Authenticator& GetInstance() {
        static Authenticator instance;
        return instance;
    }

    void Init(const unsigned char *iv, const ServerAuthOption &option) {
        options_ = option;
        std::memcpy(iv_, iv, AES_BLOCK_SIZE);
        lastKeyExpiration_ = TimeUtility::GetTimeofDaySec() +
            options_.lastKeyTTL;
    }

    bool VerifyCredential(const curve::mds::auth::Token &token) const;

 private:
    Authenticator() {}
    Authenticator(const Authenticator&) = delete;
    Authenticator& operator=(const Authenticator&) = delete;

    bool LastKeyExpired() const {
        return TimeUtility::GetTimeofDaySec() > lastKeyExpiration_;
    }

 private:
    ServerAuthOption options_;
    uint64_t lastKeyExpiration_;
    unsigned char iv_[AES_BLOCK_SIZE];
};

}   // namespace common
}   // namespace curve

#endif  // SRC_COMMON_AUTHENTICATOR_H_

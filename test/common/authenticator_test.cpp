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
 * Created Date: Thursday November 29th 2018
 * Author: yangyaokai
 */

#include <gtest/gtest.h>
#include <string>

#include "proto/auth.pb.h"
#include "src/common/authenticator.h"

namespace curve {
namespace common {

TEST(EncryptorTEST, basic_test) {
    std::string key = "123456";
    std::string data = "/data/123";
    std::string sig = Encryptor::CalcString2Signature(data, key);
    std::string expect = "ZKNsnF9DXRxeb0+xTgFD2zLYkQnE6Sy/g2ebqWEAdlc=";
    ASSERT_STREQ(sig.c_str(), expect.c_str());
}

TEST(EncryptorTEST, aes_test) {
    std::string key = "0123456789abcdef";
    unsigned char enciv[AES_BLOCK_SIZE] = {0};
    unsigned char deciv[AES_BLOCK_SIZE] = {0};
    std::string data = "hello world! hello world! hello world! hello world!";
    std::string encStr, decStr;
    int ret = Encryptor::AESEncrypt(key, enciv, data, &encStr);
    ASSERT_EQ(0, ret);
    ret = Encryptor::AESDecrypt(key, deciv, encStr, &decStr);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(data.size(), decStr.size());
    ASSERT_STREQ(data.c_str(), decStr.c_str());
}

TEST(EncryptorTEST, md532_test) {
    std::string data = "curve";
    std::string md5Str = Encryptor::MD532Digest(data);
    std::string expectStr = "4efa264f5ef3e1a5c95736e07544ebf0";
    ASSERT_EQ(md5Str, expectStr);
}

TEST(EncryptorTEST, md516_test) {
    std::string data = "curve";
    std::string md5Str = Encryptor::MD516Digest(data);
    std::string expectStr = "5ef3e1a5c95736e0";
    ASSERT_EQ(md5Str, expectStr);
}

TEST(EncryptorTEST, base64_test) {
    std::string data = "curve\ncurve";
    std::string base64Str = Encryptor::Base64Encode(data);
    LOG(INFO) << base64Str;
    std::string decodeStr = Encryptor::Base64Decode(base64Str);
    ASSERT_EQ(decodeStr, data);
}

TEST(AuthenticatorTEST, base_test) {
    // init
    std::string serverKey = "5ef3e1a5c95736e0";
    std::string fakeServerKey = "1111111111111111";
    std::string serverLastKey = "5ef3e1a5c95736e1";
    std::string sessionKey = "5ef3e1a5c95736e2";

    std::string cId = "admin";
    std::string sId = "mds";

    ServerAuthOption conf;

    curve::mds::auth::Ticket ticket;
    ticket.set_cid(cId);
    ticket.set_sid(sId);
    ticket.set_sessionkey(sessionKey);
    ticket.set_caps("*");
    ticket.set_expiration(curve::common::TimeUtility::GetTimeofDaySec() + 3);
    std::string ticketStr;
    ASSERT_TRUE(ticket.SerializeToString(&ticketStr));

    std::string encTicket;
    ASSERT_EQ(0, Encryptor::AESEncrypt(serverKey, ZEROIV, ticketStr,
        &encTicket));
    curve::mds::auth::ClientIdentity cIdentity;
    cIdentity.set_cid(cId);
    cIdentity.set_timestamp(curve::common::TimeUtility::GetTimeofDaySec());
    std::string cIdentityStr;
    ASSERT_TRUE(cIdentity.SerializeToString(&cIdentityStr));
    std::string encId;
    ASSERT_EQ(0, Encryptor::AESEncrypt(sessionKey, ZEROIV, cIdentityStr,
        &encId));

    curve::mds::auth::Token token;

    // 1. enable auth = false, verify = true
    Authenticator::GetInstance().Init(ZEROIV, conf);
    EXPECT_TRUE(Authenticator::GetInstance().VerifyCredential(token));

    // 2. enable auth = true, info miss, verify = false
    conf.enable = true;
    conf.key = serverKey;
    Authenticator::GetInstance().Init(ZEROIV, conf);
    EXPECT_FALSE(Authenticator::GetInstance().VerifyCredential(token));

    // 3. enable auth = true, verify = true
    token.set_encclientidentity(encId);
    token.set_encticket(encTicket);
    EXPECT_TRUE(Authenticator::GetInstance().VerifyCredential(token));

    // 4. descrypt failed
    conf.key = "11111";
    Authenticator::GetInstance().Init(ZEROIV, conf);
    EXPECT_FALSE(Authenticator::GetInstance().VerifyCredential(token));

    // 5. current key descrypt failed but last key success
    conf.key = fakeServerKey;
    conf.lastKey = serverKey;
    conf.lastKeyTTL = 3;
    Authenticator::GetInstance().Init(ZEROIV, conf);
    EXPECT_TRUE(Authenticator::GetInstance().VerifyCredential(token));

    // 6. current key descrypt failed and last key expiration
    conf.lastKeyTTL = 0;
    Authenticator::GetInstance().Init(ZEROIV, conf);
    sleep(1);
    EXPECT_FALSE(Authenticator::GetInstance().VerifyCredential(token));
}

}  // namespace common
}  // namespace curve

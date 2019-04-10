/*
 * Project: curve
 * File Created: Monday, 1st April 2019 5:15:20 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#ifndef SRC_COMMON_AUTHENTICATOR_H_
#define SRC_COMMON_AUTHENTICATOR_H_

#include <openssl/hmac.h>
#include <openssl/x509.h>

#include <string>

namespace curve {
namespace common {

static char b[] =
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
/* 0000000000111111111122222222223333333333444444444455555555556666 */
/* 0123456789012345678901234567890123456789012345678901234567890123 */

class Authenticator {
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

 private:
    static void HMacSha256(
        const unsigned char *text,      /* pointer to data stream        */
        int                 text_len,   /* length of data stream         */
        const unsigned char *key,       /* pointer to authentication key */
        int                 key_len,    /* length of authentication key  */
        void                *digest);

    static char* Base64(const unsigned char *src, size_t sz);
};
}   // namespace common
}   // namespace curve

#endif  // SRC_COMMON_AUTHENTICATOR_H_

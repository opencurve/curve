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
 * File Created: Monday, 1st April 2019 5:15:34 pm
 * Author: tongguangxun
 */

// function HMacSha256() is copy from brpc project:
//
// Copyright (c) 2016 Baidu, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Ge,Jun (gejun@baidu.com)
//          Jiashun Zhu (zhujiashun@baidu.com)

#ifdef OPENSSL_NO_SHA256
#undef OPENSSL_NO_SHA256
#endif

#include "src/common/authenticator.h"

#include <glog/logging.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/x509.h>
#include <string.h>

// Older openssl does not have EVP_sha256. To make the code always compile,
// we mark the symbol as weak. If the runtime does not have the function,
// handshaking will fallback to the simple one.
extern "C" {
const EVP_MD* __attribute__((weak)) EVP_sha256(void);
}

namespace curve {
namespace common {

static char b[] =
"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
/* 0000000000111111111122222222223333333333444444444455555555556666 */
/* 0123456789012345678901234567890123456789012345678901234567890123 */

std::string Authenticator::CalcString2Signature(const std::string& in,
                                    const std::string& secretKey) {
    std::string signature;
    unsigned char digest[BUFSIZ];
    memset(digest, 0x00, BUFSIZ);

    int ret = HMacSha256((unsigned char*)secretKey.c_str(), secretKey.size(),
                         (unsigned char*)in.c_str(), in.size(), digest);
    if (ret == 0) {
        signature = Base64(digest, SHA256_DIGEST_LENGTH);
    }

    return signature;
}

std::string Authenticator::GetString2Signature(uint64_t date,
                                        const std::string& owner) {
    std::string ret;
    return ret.append(std::to_string(date))
              .append(":")
              .append(owner);
}

int Authenticator::HMacSha256(const void* key, int key_size,
                               const void* data, int data_size,
                               void* digest) {
    if (NULL == EVP_sha256) {
        LOG(ERROR) << "Fail to find EVP_sha256.";
        return -1;
    }
    unsigned int digest_size = 0;
    unsigned char* temp_digest = (unsigned char*)digest;
    if (key == NULL) {
        // NOTE: first parameter of EVP_Digest in older openssl is void*.
        if (EVP_Digest(const_cast<void*>(data), data_size, temp_digest,
                       &digest_size, EVP_sha256(), NULL) < 0) {
            LOG(ERROR) << "Fail to EVP_Digest";
            return -1;
        }
    } else {
        // Note: following code uses HMAC_CTX previously which is ABI
        // inconsistent in different version of openssl.
        if (HMAC(EVP_sha256(), key, key_size,
                 (const unsigned char*) data, data_size,
                 temp_digest, &digest_size) == NULL) {
            LOG(ERROR) << "Fail to HMAC";
            return -1;
        }
    }
    if (digest_size != 32) {
        LOG(ERROR) << "digest_size=" << digest_size << " of sha256 is not 32";
        return -1;
    }
    return 0;
}

std::string Authenticator::Base64(const unsigned char *src, size_t sz) {
    unsigned char               *pp, *p, *q;
    unsigned char               *qq = NULL;
    size_t                      i, safe = sz;

    if (!src || (sz == 0))
        return (NULL);

    if ((sz % 3) == 1) {
        p = (unsigned char *)malloc(sz + 2);
        if (!p)
            return (NULL);
        memcpy(p, src, sz);
        p[sz] = p[sz + 1] = '=';
        sz += 2;
    } else if ((sz % 3) == 2) {
        p = (unsigned char *)malloc(sz + 1);
        if (!p)
            return (NULL);
        memcpy(p, src, sz);
        p[sz] = '=';
        sz++;
    } else {
        p = (unsigned char *)src;
    }

    q = (unsigned char *)malloc((sz / 3) * 4 + 2);
    if (!q) {
        if (p != src)
            free(p);
        return (NULL);
    }

    pp = p;
    qq = q;
    for (i = 0; i < sz; i += 3) {
        q[0] = b[(p[0] & 0xFC) >> 2];
        q[1] = b[((p[0] & 0x03) << 4) | ((p[1] & 0xF0) >> 4)];
        q[2] = b[((p[1] & 0x0F) << 2) | ((p[2] & 0xC0) >> 6)];
        q[3] = b[p[2] & 0x3F];
        p += 3;
        q += 4;
    }
    *q = 0;
    if ((safe % 3) == 1) {
        *(q - 1) = '=';
        *(q - 2) = '=';
    }
    if ((safe % 3) == 2)
        *(q - 1) = '=';

    if (pp != src)
        free(pp);

    std::string ret = std::string((char*)qq);    // NOLINT
    free(qq);
    qq = NULL;
    return ret;
}
}   // namespace common
}   // namespace curve

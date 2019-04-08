/*
 * Project: curve
 * File Created: Monday, 1st April 2019 5:15:34 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#ifdef OPENSSL_NO_SHA256
#undef OPENSSL_NO_SHA256
#endif

#include <openssl/sha.h>

#include <string.h>

#include "src/common/authenticator.h"

namespace curve {
namespace common {
std::string Authenticator::CalcString2Signature(const std::string& in,
                                    const std::string& secretKey) {
    std::string signature;
    unsigned char digest[BUFSIZ];
    memset(digest, 0x00, BUFSIZ);

    HMacSha256((unsigned char*)in.c_str(), in.size(),
            (unsigned char*)secretKey.c_str(), secretKey.size(), digest);
    signature = Base64(digest, SHA256_DIGEST_LENGTH);

    return signature;
}

std::string Authenticator::GetString2Signature(uint64_t date,
                                        const std::string& owner) {
    std::string ret;
    return ret.append(std::to_string(date))
              .append(":")
              .append(owner);
}

void Authenticator::HMacSha256(
            const unsigned char *text,      /* pointer to data stream        */
            int                 text_len,   /* length of data stream         */
            const unsigned char *key,       /* pointer to authentication key */
            int                 key_len,    /* length of authentication key  */
            void                *digest) {
    unsigned char k_ipad[65];   /* inner padding key XORd with ipad */
    unsigned char k_opad[65];   /* outer padding key XORd with opad */
    unsigned char tk[SHA256_DIGEST_LENGTH];
    unsigned char tk2[SHA256_DIGEST_LENGTH];
    unsigned char bufferIn[1024];
    unsigned char bufferOut[1024];
    int           i;

    /* if key is longer than 64 bytes reset it to key=sha256(key) */
    if (key_len > 64) {
        SHA256(key, key_len, tk);
        key     = tk;
        key_len = SHA256_DIGEST_LENGTH;
    }

    /*
     * the HMAC_SHA256 transform looks like:
     *
     * SHA256(K XOR opad, SHA256(K XOR ipad, text))
     *
     * where K is an n byte key
     * ipad is the byte 0x36 repeated 64 times
     * opad is the byte 0x5c repeated 64 times
     * and text is the data being protected
     */

    /* start out by storing key in pads */
    memset(k_ipad, 0, sizeof k_ipad);
    memset(k_opad, 0, sizeof k_opad);
    memcpy(k_ipad, key, key_len);
    memcpy(k_opad, key, key_len);

    /* XOR key with ipad and opad values */
    for (i = 0; i < 64; i++) {
        k_ipad[i] ^= 0x36;
        k_opad[i] ^= 0x5c;
    }

    /*
     * perform inner SHA256
     */
    memset(bufferIn, 0x00, 1024);
    memcpy(bufferIn, k_ipad, 64);
    memcpy(bufferIn + 64, text, text_len);

    SHA256(bufferIn, 64 + text_len, tk2);

    /*
     * perform outer SHA256
     */
    memset(bufferOut, 0x00, 1024);
    memcpy(bufferOut, k_opad, 64);
    memcpy(bufferOut + 64, tk2, SHA256_DIGEST_LENGTH);

    SHA256(bufferOut, 64 + SHA256_DIGEST_LENGTH, (unsigned char*)digest);
}

char* Authenticator::Base64(const unsigned char *src, size_t sz) {
    unsigned char               *pp, *p, *q;
    static unsigned char *qq = NULL;
    size_t                      i, safe = sz;

    if (qq) {
        free(qq);
        qq = NULL;
    }
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
    *q = NULL;
    if ((safe % 3) == 1) {
        *(q - 1) = '=';
        *(q - 2) = '=';
    }
    if ((safe % 3) == 2)
        *(q - 1) = '=';

    if (pp != src)
        free(pp);
    return ((char *)qq);    // NOLINT
}
}   // namespace common
}   // namespace curve

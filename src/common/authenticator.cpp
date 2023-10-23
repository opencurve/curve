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

#include <openssl/aes.h>
#include <memory>
#ifdef OPENSSL_NO_SHA256
#undef OPENSSL_NO_SHA256
#endif

#include <openssl/md5.h>
#include <glog/logging.h>
#include <openssl/hmac.h>
#include <openssl/x509.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <string.h>
#include <cstring>
#include <algorithm>
#include "src/common/authenticator.h"

// Older openssl does not have EVP_sha256. To make the code always compile,
// we mark the symbol as weak. If the runtime does not have the function,
// handshaking will fallback to the simple one.
extern "C" {
const EVP_MD* __attribute__((weak)) EVP_sha256(void);
}

namespace curve {
namespace common {

using ::curve::mds::auth::Token;
using ::curve::mds::auth::Ticket;
using ::curve::mds::auth::ClientIdentity;

const unsigned char ZEROIV[AES_BLOCK_SIZE] = {0};
const std::string AUTH_ROLE = "auth";                     // NOLINT
const std::string MDS_ROLE = "mds";                       // NOLINT
const std::string CHUNKSERVER_ROLE = "chunkserver";       // NOLINT
const std::string SNAPSHOTCLONE_ROLE = "snapshotclone";   // NOLINT

std::string Encryptor::CalcString2Signature(const std::string& in,
                                    const std::string& secretKey) {
    std::string signature;
    unsigned char digest[BUFSIZ];
    memset(digest, 0x00, BUFSIZ);

    int ret = HMacSha256((unsigned char*)secretKey.c_str(), secretKey.size(),
                         (unsigned char*)in.c_str(), in.size(), digest);
    if (ret == 0) {
        signature = Base64Encode(digest, SHA256_DIGEST_LENGTH);
    }

    return signature;
}

std::string Encryptor::GetString2Signature(uint64_t date,
                                        const std::string& owner) {
    std::string ret;
    return ret.append(std::to_string(date))
              .append(":")
              .append(owner);
}

int Encryptor::HMacSha256(const void* key, int key_size,
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

std::string Encryptor::Base64Encode(const std::string &src) {
    return Base64Encode(reinterpret_cast<const unsigned char*>(src.c_str()),
        src.size());
}

std::string Encryptor::Base64Encode(const unsigned char *src, size_t sz) {
    BIO *bio, *b64;
    BUF_MEM *bufferPtr;

    b64 = BIO_new(BIO_f_base64());
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL);
    bio = BIO_new(BIO_s_mem());
    bio = BIO_push(b64, bio);

    BIO_write(bio, src, sz);
    BIO_flush(bio);

    BIO_get_mem_ptr(bio, &bufferPtr);
    BIO_set_close(bio, BIO_NOCLOSE);

    std::string result;
    result.assign(bufferPtr->data, bufferPtr->length);

    BIO_free_all(bio);
    return result;
}

std::string Encryptor::Base64Decode(const std::string& src) {
    BIO *bio, *b64;
    size_t length = src.size();
    std::unique_ptr<char[]> buffer(new char[length]);
    std::string decodedString;

    if (length % 4 != 0) {
        return decodedString;
    }

    b64 = BIO_new(BIO_f_base64());
    bio = BIO_new_mem_buf(src.c_str(), length);
    bio = BIO_push(b64, bio);

    BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
    int decodedLength = BIO_read(bio, buffer.get(), length);

    if (decodedLength > 0) {
        decodedString.assign(buffer.get(), decodedLength);
    }

    BIO_free_all(bio);

    return decodedString;
}

int Encryptor::AESEncrypt(const std::string &key,
    const unsigned char *iv, const std::string &in, std::string *out) {
    // the AES_cbc_encrypt will change iv
    unsigned char uiv[AES_BLOCK_SIZE];
    std::memcpy(uiv, iv, AES_BLOCK_SIZE);
    AES_KEY enc_key;
    const int keyBits = key.size() * 8;
    const int pad_len = AES_BLOCK_SIZE - in.size() % AES_BLOCK_SIZE;
    const int encLength = in.size() + pad_len;
    // cbc padding
    std::string data = in;
    data.resize(data.size() + pad_len, static_cast<char>(pad_len));

    int ret = AES_set_encrypt_key(
        reinterpret_cast<const unsigned char*>(key.c_str()), keyBits, &enc_key);
    if (ret != 0) {
        return ret;
    }
    std::unique_ptr<unsigned char> enc_out(new unsigned char[encLength]);
    memset(enc_out.get(), 0, sizeof(unsigned char) * encLength);
    AES_cbc_encrypt(reinterpret_cast<const unsigned char*>(data.c_str()),
        enc_out.get(), data.size(), &enc_key, uiv, AES_ENCRYPT);
    *out = std::string(reinterpret_cast<const char*>(enc_out.get()), encLength);
    return 0;
}

int Encryptor::AESDecrypt(const std::string &key,
    const unsigned char *iv, const std::string &in, std::string *out) {
    // the AES_cbc_encrypt will change iv
    unsigned char uiv[AES_BLOCK_SIZE];
    std::memcpy(uiv, iv, AES_BLOCK_SIZE);
    AES_KEY dec_key;
    const int keyBits = key.size() * 8;

    int ret = AES_set_decrypt_key(
        reinterpret_cast<const unsigned char*>(key.c_str()), keyBits, &dec_key);
    if (ret != 0) {
        return ret;
    }
    std::unique_ptr<unsigned char> dec_out(new unsigned char[in.size()]);
    memset(dec_out.get(), 0, sizeof(unsigned char) * in.size());
    AES_cbc_encrypt(reinterpret_cast<const unsigned char*>(in.c_str()),
        dec_out.get(), in.size(), &dec_key, uiv, AES_DECRYPT);
    // note: The length of the ciphertext must be an integer multiple of AES_BLOCK_SIZE,  // NOLINT
    // if it is insufficient, it needs to be filled,
    // and the number of bytes to be filled is the last byte of the ciphertext
    auto pad_len = std::min<std::uint8_t>(dec_out.get()[in.size() - 1],
        AES_BLOCK_SIZE);
    if (pad_len > AES_BLOCK_SIZE || pad_len >= in.size()) {
        return -1;
    }
    *out = std::string(reinterpret_cast<const char*>(dec_out.get()),
        in.size() - pad_len);
    return 0;
}

std::string Encryptor::MD532Digest(const std::string &data) {
    unsigned char digest[MD5_DIGEST_LENGTH];
    MD5(reinterpret_cast<const unsigned char*>(data.c_str()), data.size(),
        digest);
    char mdString[MD5_DIGEST_LENGTH * 2 + 1] = {0};
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i)
        sprintf(&mdString[i * 2], "%02x", (unsigned int)digest[i]);  // NOLINT
    return mdString;
}

std::string Encryptor::MD516Digest(const std::string &data) {
    return MD532Digest(data).substr(8, 16);
}

bool Encryptor::AESKeyValid(const std::string &key) {
    auto keyBits = key.length() * 8;
    return (keyBits == 128 || keyBits == 192 || keyBits == 256);
}

bool Authenticator::VerifyCredential(
    const curve::mds::auth::Token &token) const {
    if (!options_.enable) {
        return true;
    }

    if (token.encticket().empty() || token.encclientidentity().empty()) {
        LOG(WARNING) << "token info is not enough";
        return false;
    }
    // decrypt ticket
    Ticket ticket;
    bool needRedecByLastKey = false;
    unsigned char uiv[AES_BLOCK_SIZE];
    std::memcpy(uiv, iv_, AES_BLOCK_SIZE);
    std::string ticketStr;
    int ret = Encryptor::AESDecrypt(options_.key, uiv, token.encticket(),
        &ticketStr);
    if (ret != 0) {
        LOG(WARNING) << "aes decrypt ticket failed when VerifyCredential"
                     << "used current key, then will try last key: "
                     << options_.lastKey;
        needRedecByLastKey = true;
    } else {
        if (!ticket.ParseFromString(ticketStr)) {
            LOG(WARNING) << "aes decrypt ticket failed when VerifyCredential"
                         << "used current key, then will try last key: "
                            << options_.lastKey;
            needRedecByLastKey = true;
        }
    }
    if (needRedecByLastKey) {
        // maybe in the key rotation phase
        if (options_.lastKey.size() == 0 || LastKeyExpired()) {
            LOG(WARNING) << "aes decrypt ticket failed with current key and"
                         << " last key is empty or expiration when"
                         << " VerifyCredential, lastKey size = "
                         << options_.lastKey.size()
                         << ", lastKey expiration = " << lastKeyExpiration_;
            return false;
        } else {
            std::memcpy(uiv, iv_, AES_BLOCK_SIZE);
            ret = Encryptor::AESDecrypt(options_.lastKey, uiv,
                token.encticket(), &ticketStr);
            if (ret != 0) {
                LOG(WARNING) << "aes decrypt ticket failed with last key"
                             << "when VerifyCredential";
                return false;
            }
        }
        if (!ticket.ParseFromString(ticketStr)) {
            LOG(WARNING) << "aes decrypt ticket failed with last key"
                         << " when VerifyCredential";
            return false;
        }
        LOG(INFO) << "verify success with last key, lastKey expiration = "
                  << lastKeyExpiration_;
    }

    // check timeout
    auto now = TimeUtility::GetTimeofDaySec();
    if (ticket.expiration() < now) {
        LOG(WARNING) << "ticket is expired, now = " << now
                     << ", expiration = " << ticket.expiration();
        return false;
    }

    // decrypt client identity
    std::memcpy(uiv, iv_, AES_BLOCK_SIZE);
    std::string cInfoString;
    ret = curve::common::Encryptor::AESDecrypt(ticket.sessionkey(),
        uiv, token.encclientidentity(), &cInfoString);
    if (ret != 0) {
        LOG(WARNING) << "aes decrypt cInfo failed when VerifyCredential";
        return false;
    }
    ClientIdentity cInfo;
    if (!cInfo.ParseFromString(cInfoString)) {
        LOG(WARNING) << "aes decrypt cInfo failed when VerifyCredential";
        return false;
    }

    // check client caps if needed later here
    if (cInfo.timestamp() + options_.requestTTL < now ||
        ticket.cid() != cInfo.cid()) {
        LOG(WARNING) << "cid in ticket is not equal to decrypted from encId"
                     << " or request expiration, cid in ticket = "
                     << ticket.cid()
                     << ", decrypted cid =  " << cInfo.cid()
                     << ", reqTimestamp = " << cInfo.timestamp()
                     << ", ttl = " << options_.requestTTL
                     << ", now = " << now;
        return false;
    }
    return true;
}

}   // namespace common
}   // namespace curve

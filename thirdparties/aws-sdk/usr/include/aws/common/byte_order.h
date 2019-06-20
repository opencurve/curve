#ifndef AWS_COMMON_BYTE_ORDER_H
#define AWS_COMMON_BYTE_ORDER_H

/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <aws/common/common.h>

#ifdef _MSC_VER
#    include <stdlib.h>
#else
#    include <netinet/in.h>
#endif /* _MSC_VER */

/**
 * Returns 1 if machine is big endian, 0 if little endian.
 * If you compile with even -O1 optimization, this check is completely optimized
 * out at compile time and code which calls "if (aws_is_big_endian())" will do
 * the right thing without branching.
 */
AWS_STATIC_IMPL int aws_is_big_endian(void) {
    const uint16_t z = 0x100;
    return *(const uint8_t *)&z;
}

/**
 * Convert 64 bit integer from host to network byte order.
 */
AWS_STATIC_IMPL uint64_t aws_hton64(uint64_t x) {
    if (aws_is_big_endian()) {
        return x;
    }
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
    uint64_t v;
    __asm__("bswap %q0" : "=r"(v) : "0"(x));
    return v;
#elif defined(_MSC_VER)
    return _byteswap_uint64(x);
#else
    uint32_t low = (uint32_t)x;
    uint32_t high = (uint32_t)(x >> 32);
    return ((uint64_t)htonl(low)) << 32 | htonl(high);
#endif
}

/**
 * Convert 64 bit integer from network to host byte order.
 */
AWS_STATIC_IMPL uint64_t aws_ntoh64(uint64_t x) {
    return aws_hton64(x);
}

/**
 * Convert 32 bit integer from host to network byte order.
 */
AWS_STATIC_IMPL uint32_t aws_hton32(uint32_t x) {
#ifdef _MSC_VER
    return aws_is_big_endian() ? x : _byteswap_ulong(x);
#else
    return htonl(x);
#endif
}

/**
 * Convert 32 bit integer from network to host byte order.
 */
AWS_STATIC_IMPL uint32_t aws_ntoh32(uint32_t x) {
#ifdef _MSC_VER
    return aws_is_big_endian() ? x : _byteswap_ulong(x);
#else
    return ntohl(x);
#endif
}

/**
 * Convert 16 bit integer from host to network byte order.
 */
AWS_STATIC_IMPL uint16_t aws_hton16(uint16_t x) {
#ifdef _MSC_VER
    return aws_is_big_endian() ? x : _byteswap_ushort(x);
#else
    return htons(x);
#endif
}

/**
 * Convert 16 bit integer from network to host byte order.
 */
AWS_STATIC_IMPL uint16_t aws_ntoh16(uint16_t x) {
#ifdef _MSC_VER
    return aws_is_big_endian() ? x : _byteswap_ushort(x);
#else
    return ntohs(x);
#endif
}

#endif /* AWS_COMMON_BYTE_ORDER_H */

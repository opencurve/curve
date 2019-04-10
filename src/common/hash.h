/*
 * Project: curve
 * Created Date: 2019-3-25
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */

// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Simple hash function used for internal data structures

#ifndef SRC_COMMON_HASH_H_
#define SRC_COMMON_HASH_H_

#include <stddef.h>
#include <stdint.h>

// The FALLTHROUGH_INTENDED macro can be used to annotate implicit fall-through
// between switch labels. The real definition should be provided externally.
// This one is a fallback version for unsupported compilers.
#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED do { } while (0)
#endif

namespace curve {
namespace common {
uint32_t Hash(const char* data, size_t n, uint32_t seed) {
    // Similar to murmur hash
    const uint32_t m = 0xc6a4a793;
    const uint32_t r = 24;
    const char* limit = data + n;
    uint32_t h = seed ^ (n * m);

    // Pick up four bytes at a time
    while (data + 4 <= limit) {
        uint32_t w = *reinterpret_cast<const uint32_t*>(data);
        data += 4;
        h += w;
        h *= m;
        h ^= (h >> 16);
    }

    // Pick up remaining bytes
    switch (limit - data) {
        case 3:
            h += static_cast<unsigned char>(data[2]) << 16;
            FALLTHROUGH_INTENDED;
        case 2:
            h += static_cast<unsigned char>(data[1]) << 8;
            FALLTHROUGH_INTENDED;
        case 1:
            h += static_cast<unsigned char>(data[0]);
            h *= m;
            h ^= (h >> r);
            break;
    }
    return h;
}
}  // namespace common
}  // namespace curve

#endif  // SRC_COMMON_HASH_H_

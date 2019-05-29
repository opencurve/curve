/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

/*
 * This header is already included, but include it again to make editor
 * highlighting happier.
 */
#include <aws/common/common.h>

/**
 * Multiplies a * b. If the result overflows, returns 2^64 - 1.
 */
AWS_STATIC_IMPL uint64_t aws_mul_u64_saturating(uint64_t a, uint64_t b) {
    uint64_t x = a * b;
    if (a != 0 && (a > 0xFFFFFFFF || b > 0xFFFFFFFF) && x / a != b) {
        return ~(uint64_t)0;
    }
    return x;
}

/**
 * Multiplies a * b and returns the truncated result in *r. If the result
 * overflows, returns 0, else returns 1.
 */
AWS_STATIC_IMPL int aws_mul_u64_checked(uint64_t a, uint64_t b, uint64_t *r) {
    uint64_t x = a * b;
    *r = x;
    if (a != 0 && (a > 0xFFFFFFFF || b > 0xFFFFFFFF) && x / a != b) {
        return 0;
    }
    return 1;
}

/**
 * Multiplies a * b. If the result overflows, returns 2^32 - 1.
 */
AWS_STATIC_IMPL uint32_t aws_mul_u32_saturating(uint32_t a, uint32_t b) {
    uint32_t x = a * b;
    if (a != 0 && (a > 0xFFFF || b > 0xFFFF) && x / a != b) {
        return ~(uint32_t)0;
    }
    return x;
}

/**
 * Multiplies a * b and returns the result in *r. If the result overflows,
 * returns 0, else returns 1.
 */
AWS_STATIC_IMPL int aws_mul_u32_checked(uint32_t a, uint32_t b, uint32_t *r) {
    uint32_t x = a * b;
    *r = x;
    if (a != 0 && (a > 0xFFFF || b > 0xFFFF) && x / a != b) {
        return 0;
    }
    return 1;
}
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

#include <immintrin.h>
#include <intrin.h>

/**
 * Multiplies a * b. If the result overflows, returns 2^64 - 1.
 */
AWS_STATIC_IMPL uint64_t aws_mul_u64_saturating(uint64_t a, uint64_t b) {
    uint64_t out;
    uint64_t ret_val = _umul128(a, b, &out);
    return out == 0 ? ret_val : ~(uint64_t)0;
}

/**
 * Multiplies a * b and returns the truncated result in *r. If the result
 * overflows, returns 0, else returns 1.
 */
AWS_STATIC_IMPL int aws_mul_u64_checked(uint64_t a, uint64_t b, uint64_t *r) {
    uint64_t out;
    *r = _umul128(a, b, &out);

    return out == 0;
}

/**
 * Multiplies a * b. If the result overflows, returns 2^32 - 1.
 */
AWS_STATIC_IMPL uint32_t aws_mul_u32_saturating(uint32_t a, uint32_t b) {
    uint32_t out;
    uint32_t ret_val = _mulx_u32(a, b, &out);
    return out == 0 ? ret_val : ~(uint32_t)0;
}

/**
 * Multiplies a * b and returns the result in *r. If the result overflows,
 * returns 0, else returns 1.
 */
AWS_STATIC_IMPL int aws_mul_u32_checked(uint32_t a, uint32_t b, uint32_t *r) {
    uint32_t out;
    *r = _mulx_u32(a, b, &out);

    return out == 0;
}
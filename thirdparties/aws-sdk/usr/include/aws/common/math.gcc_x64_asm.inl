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
    /* We can use inline assembly to do this efficiently on x86-64 and x86.

    we specify rdx as an output, rather than a clobber, because we want to
    allow it to be allocated as an input register */

    uint64_t rdx;
    __asm__("mulq %q[arg2]\n" /* rax * b, result is in RDX:RAX, OF=CF=(RDX != 0) */
            "cmovc %q[saturate], %%rax\n"
            : /* in/out: %rax = a, out: rdx (ignored) */ "+&a"(a), "=&d"(rdx)
            : /* in: register only */ [arg2] "r"(b),
              /* in: saturation value (reg/memory) */ [saturate] "rm"(~0LL)
            : /* clobbers: cc */ "cc");
    (void)rdx; /* suppress unused warnings */
    return a;
}

/**
 * Multiplies a * b and returns the truncated result in *r. If the result
 * overflows, returns 0, else returns 1.
 */
AWS_STATIC_IMPL int aws_mul_u64_checked(uint64_t a, uint64_t b, uint64_t *r) {
    /* We can use inline assembly to do this efficiently on x86-64 and x86. */

    int flag;
    uint64_t result = a;
    __asm__("mulq %q[arg2]\n"    /* rax * b, result is in RDX:RAX, OF=CF=(RDX != 0) */
            "setno %%dl\n"       /* rdx = (OF = 0) */
            "and $0xFF, %%edx\n" /* mask out flag */
            : /* in/out: %rax (with first operand) */ "+&a"(result), "=&d"(flag)
            : /* in: reg for 2nd operand */
            [arg2] "r"(b)
            : /* clobbers: cc */ "cc");
    *r = result;
    return flag;
}

/**
 * Multiplies a * b. If the result overflows, returns 2^32 - 1.
 */
AWS_STATIC_IMPL uint32_t aws_mul_u32_saturating(uint32_t a, uint32_t b) {
    /* We can use inline assembly to do this efficiently on x86-64 and x86.

     we specify edx as an output, rather than a clobber, because we want to
    allow it to be allocated as an input register */
    uint32_t edx;
    __asm__("mull %k[arg2]\n" /* eax * b, result is in EDX:EAX, OF=CF=(EDX != 0) */
            /* cmov isn't guaranteed to be available on x86-32 */
            "jnc .1f%=\n"
            "mov $0xFFFFFFFF, %%eax\n"
            ".1f%=:"
            : /* in/out: %eax = result/a, out: edx (ignored) */ "+&a"(a), "=&d"(edx)
            : /* in: operand 2 in reg */ [arg2] "r"(b)
            : /* clobbers: cc */ "cc");
    (void)edx; /* suppress unused warnings */
    return a;
}

/**
 * Multiplies a * b and returns the result in *r. If the result overflows,
 * returns 0, else returns 1.
 */
AWS_STATIC_IMPL int aws_mul_u32_checked(uint32_t a, uint32_t b, uint32_t *r) {
    /* We can use inline assembly to do this efficiently on x86-64 and x86. */
    uint32_t result = a;
    int flag;
    /**
     * Note: We use SETNO which only takes a byte register. To make this easy,
     * we'll write it to dl (which we throw away anyway) and mask off the high bits.
     */
    __asm__("mull %k[arg2]\n"    /* eax * b, result is in EDX:EAX, OF=CF=(EDX != 0) */
            "setno %%dl\n"       /* flag = !OF ^ (junk in top 24 bits) */
            "and $0xFF, %%edx\n" /* flag = flag & 0xFF */
            /* we allocate flag to EDX since it'll be clobbered by MUL anyway */
            : /* in/out: %eax = a, %dl = flag */ "+&a"(result), "=&d"(flag)
            : /* reg = b */ [arg2] "r"(b)
            : /* clobbers: cc */ "cc");
    *r = result;
    return flag;
}

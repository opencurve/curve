#ifndef AWS_COMMON_CONFIG_H
#define AWS_COMMON_CONFIG_H

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
 * This header exposes compiler feature test results determined during cmake
 * configure time to inline function implementations. The macros defined here
 * should be considered to be an implementation detail, and can change at any
 * time.
 */
#define AWS_HAVE_GCC_OVERFLOW_MATH_EXTENSIONS
#define AWS_HAVE_GCC_INLINE_ASM
/* #undef AWS_ENABLE_HW_OPTIMIZATION */
/* #undef AWS_HAVE_MSVC_MULX */

#endif

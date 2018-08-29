/*
 * Project: curve
 * File Created: Wednesday, 5th September 2018 7:23:56 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef CURVE_COMPILER_SPECFIC_H
#define CURVE_COMPILER_SPECFIC_H

// Cacheline related --------------------------------------
#define CURVE_CACHELINE_SIZE 64
#define CURVE_CACHELINE_ALIGNMENT alignas(CURVE_CACHELINE_SIZE)


// branch predict
#if defined(COMPILER_GCC)
#  if defined(__cplusplus)
#    define CURVE_LIKELY(expr) (__builtin_expect(static_cast<bool>(expr), true))
#    define CURVE_UNLIKELY(expr) (__builtin_expect(static_cast<bool>(expr), false))
#  else
#    define CURVE_LIKELY(expr) (__builtin_expect(!!(expr), 1))
#    define CURVE_UNLIKELY(expr) (__builtin_expect(!!(expr), 0))
#  endif
#else
#  define CURVE_LIKELY(expr) (expr)
#  define CURVE_UNLIKELY(expr) (expr)
#endif

#endif

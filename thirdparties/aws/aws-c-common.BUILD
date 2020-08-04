# Description:
# AWS C++ SDK

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

load("@curve//:common.bzl", "template_rule")

cc_library(
    name = "aws_c_common",
    srcs = glob([
        "source/*.c",
        "source/posix/*.c",
    ]),
    hdrs = [
        "include/aws/common/config.h"
    ] + glob([
        "include/**/*.h",
        "include/aws/common/**/*.inl"
    ]),
    linkopts = [
    ],
    includes = [
        "include/",
    ],
    deps = [
    ],
    copts = [
        "-std=c11",
        "-D _GNU_SOURCE",
    ],
)

template_rule(
    name = "config_h",
    src = "include/aws/common/config.h.in",
    out = "include/aws/common/config.h",
    substitutions = {
        "cmakedefine AWS_HAVE_GCC_OVERFLOW_MATH_EXTENSIONS": "undef AWS_HAVE_GCC_OVERFLOW_MATH_EXTENSIONS",
        "cmakedefine AWS_HAVE_GCC_INLINE_ASM": "define AWS_HAVE_GCC_INLINE_ASM",
        "cmakedefine AWS_HAVE_MSVC_MULX": "undef AWS_HAVE_MSVC_MULX",
        "cmakedefine AWS_HAVE_EXECINFO": "define AWS_HAVE_EXECINFO",
    },
)

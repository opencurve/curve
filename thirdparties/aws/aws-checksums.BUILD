# Description:
#   AWS C++ SDK

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

cc_library(
    name = "aws_checksums",
    srcs = glob([
        "source/intel/*.c",
        "source/arm/*.c",
        "source/*.c",
    ]),
    hdrs = glob([
        "include/**/*.h"
    ]),
    includes = [
        "include/",
    ],
    deps = [
        "@aws_c_common",
    ],
    defines = [
        "DEBUG_BUILD",
    ],
    copts = [
        "-std=c11",
    ],
)

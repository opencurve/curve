# Description:
#   AWS C++ SDK

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE"])

cc_library(
    name = "aws_c_event_stream",
    srcs = glob([
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
        "@aws_checksums",
    ],
    copts = [
        "-std=c11",
    ],
)

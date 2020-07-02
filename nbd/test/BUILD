COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_SSE42",
    "-DNDEBUG",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-parameter",
    "-Wno-unused-variable",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
    "-Wno-missing-field-initializers",
    "-std=c++14",
    "-I /usr/include/libnl3",
]

cc_library(
    name = "mock_lib",
    srcs = glob([
        "mock_*",
        "fake_*",
    ]),
    hdrs = glob(["*.h"]),
    deps = [
        "//external:gtest",
        "//nbd/src:curvenbd",
    ],
    copts = COPTS,
    visibility = ["//visibility:public"],
)

cc_binary(
    name = "nbd_test",
    srcs = glob([
        "*.cpp",
    ]),
    deps = [
        "//nbd/src:curvenbd",
        "//nbd/test:mock_lib"
    ],
    copts = COPTS,
)

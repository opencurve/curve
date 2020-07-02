COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_ZLIB",
    "-DHAVE_SSE42",
    "-DNDEBUG",
    "-std=c++14",
    "-pipe",
    "-W",
    "-Wall",
    "-Wno-unused-parameter",
    "-fPIC",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-variable",
    "-Wno-missing-field-initializers",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
]

cc_library(
    name = "nebdclient",
    srcs = glob([
        "*.h",
        "*.cpp",
        ]
    ),
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//external:brpc",
        "//nebd/src/common:nebd_common",
        "//nebd/proto:client_cc_proto"
    ],
    copts = COPTS,
    linkopts = ["-Wl,-rpath=/usr/lib/nebd"],
)

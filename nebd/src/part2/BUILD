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
    "-Wnon-virtual-dtor"
]

cc_library(
    name = "nebdserver",
    srcs = glob([
        "*.h",
        "*.cpp",
        ],exclude = ["main.cpp"]
    ),
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//external:brpc",
        "//external:json",
        "//external:gflags",
        "//src/client:curve",
        "//src/common:curve_common",
        "//src/common/concurrent:curve_concurrent",
        "//nebd/src/common:nebd_common",
        "//nebd/proto:client_cc_proto",
    ],
    copts = COPTS,
)

cc_binary(
    name = "nebd-server",
    srcs = glob([
        "main.cpp",
        ]
    ),
    deps = [
        "//nebd/src/part2:nebdserver",
    ],
    copts = COPTS,
    linkopts = ["-Wl,-rpath=/usr/lib/nebd"],
)

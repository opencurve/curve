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
]

cc_library(
    name = "curve_common",
    srcs = glob([
        "*.h",
        "*.cpp",
        ],exclude = ["authenticator.*"]
    ),
    copts = COPTS,
    linkopts = [
        "-laws-cpp-sdk-s3",
        "-laws-cpp-sdk-core",
        "-L/usr/local/lib"
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//external:brpc",
        "//external:butil",
        "//src/common/concurrent:curve_concurrent"
    ],
)

cc_library(
    name = "curve_auth",
    srcs = glob([
        "authenticator.h",
        "authenticator.cpp",
    ]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//external:butil",
        "//src/common/concurrent:curve_concurrent"
    ],
)

cc_library(
    name = "curve_uncopy",
    srcs = glob([
        "uncopyable.h",
    ]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
    ],
)

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
    name = "test-utils",
    srcs = glob([
        "count_down_event.h",
    ]),
    copts = COPTS,
    linkopts = [
    ],
    visibility = ["//visibility:public"],
)

cc_test(
    name = "test-utils-test",
    srcs = glob([
        "count_down_event_test.cpp",
    ]),
    deps = [
        "//test/utils:test-utils",
        "@com_google_googletest//:gtest_main",
    ],
)

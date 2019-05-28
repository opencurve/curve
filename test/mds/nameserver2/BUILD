cc_test(
    name = "curvefs_test",
    srcs = glob(["*.cpp", "*.h"]),
    copts = ["-g","-DHAVE_ZLIB=1"],
    deps = [
            "//src/mds/nameserver2:nameserver2",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            "//external:brpc",
            "//src/common:curve_common",
            "//src/mds/topology:topology",
            "//test/mds/mock:common_mock",
    ],
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
cc_library(
    name = "fakes",
    srcs = [],
    hdrs = ["fakes.h"],
    deps = [
        "//src/mds/topology:topology",
        "//src/mds/nameserver2:nameserver2",
    ],
    visibility = ["//visibility:public"],
)

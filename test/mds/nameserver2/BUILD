cc_test(
    name = "curvefs_test",
    srcs = glob(["*.cpp", "*.h"]),
    copts = ["-g","-DHAVE_ZLIB=1"],
    deps = [
            "//src/mds/nameserver2:nameserver2",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            "//external:brpc",
            "//src/common:common-lib",
            "//src/mds/topology:topology"
    ],
)

cc_test(
    name = "alloc_statistic_test",
    srcs = glob(["*.cpp", "*.h"]),
    copts = ["-g","-DHAVE_ZLIB=1"],
    deps = [
            "//src/mds/nameserver2:nameserver2",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            "//src/common:curve_common",
            "//src/mds/nameserver2/allocstatistic:alloc_statistic",
            "//test/mds/mock:common_mock",
            "//src/mds/nameserver2/helper:helper",
    ],
)
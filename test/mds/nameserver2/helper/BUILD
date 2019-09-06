cc_test(
    name = "helper_test",
    srcs = glob(["*.cpp", "*.h"]),
    copts = ["-g","-DHAVE_ZLIB=1"],
    linkopts = ["-lfiu"],
    deps = [
            "//src/mds/nameserver2:nameserver2",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            "//src/common:curve_common",
            "//src/mds/nameserver2/helper:helper",
    ],
)
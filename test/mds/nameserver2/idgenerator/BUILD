cc_test(
    name = "id_generator_test",
    srcs = glob(["*.cpp", "*.h"]),
    copts = ["-g","-DHAVE_ZLIB=1"],
    deps = [
            "//src/mds/nameserver2:nameserver2",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            "//src/common:curve_common",
            "//src/mds/nameserver2/helper:helper",
            "//src/mds/nameserver2/idgenerator:idgenerator",
            "//test/mds/mock:common_mock",
            "//test/mds/nameserver2/mock:nameserver2_mock",
    ],
)
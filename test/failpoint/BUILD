cc_test(
    name = "failpoint_test",
    srcs = glob(["*.cpp", "*.h"]),
    copts = ["-g","-DHAVE_ZLIB=1", "-DFIU_ENABLE=1", "-DFIU_TRACE=1"],
    deps = [
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
    ],
    linkopts = ["-lfiu"],
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
# cc_library(
#     name = "fakes",
#     srcs = [],
#     hdrs = ["fakes.h"],
#     deps = [
#         "//src/mds/topology:topology",
#          "//src/mds/nameserver2:nameserver2",
#     ],
#     visibility = ["//visibility:public"],
# )

cc_test(
    name = "leader_election_test",
    srcs = glob(["*.cpp", "*.h"]),
    copts = ["-g","-DHAVE_ZLIB=1"],
    defines = ["FIU_ENABLE"],
    deps = [
            "//src/mds/leader_election:leader_election_for_test",
            "//src/mds/nameserver2/helper",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            "//test/mds/mock:common_mock",
    ],
    linkopts = ["-lfiu"],
)

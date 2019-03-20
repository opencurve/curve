cc_test(
    name = "repo-test",
    srcs = glob(["*.cpp"]),
    linkopts = [
        "-lmysqlcppconn",
    ],
    deps = [
        "//external:json",
        "//src/mds/dao:mdsrepo",
        "@com_google_googletest//:gtest",
    ],
)

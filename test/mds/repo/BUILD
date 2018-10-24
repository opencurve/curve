cc_test(
    name = "repo-test",
    srcs = glob(["*.cpp"]),
    linkopts = [
        "-lmysqlcppconn",
    ],
    deps = [
        "//external:json",
        "//src/mds/repo",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "repo-test",
    srcs = glob(["*.cpp"]),
    linkopts = [
        "-lmysqlcppconn",
    ],
    deps = [
        "//external:json",
        "//src/repo:repo",
        "@com_google_googletest//:gtest",
    ],
)

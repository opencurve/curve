load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS"
)

cc_test(
    name = "schedule_service_test",
    srcs = ["scheduleService_test.cpp"],
    deps = [
        "//external:gtest",
        "//src/mds/schedule/scheduleService:scheduleservice",
        "//test/mds/mock:common_mock",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main"
    ]
)
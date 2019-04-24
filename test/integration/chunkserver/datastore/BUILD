DEPS = [
    "@com_google_googletest//:gtest",
    "//external:braft",
    "//external:gflags",
    "//include:include-common",
    "//include/chunkserver:include-chunkserver",
    "//src/common:curve_common",
    "//src/chunkserver/datastore:chunkserver_datastore",
    "//test/chunkserver/datastore:chunkfilepool_helper",
]

cc_test(
    name = "datastore_clone_case_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_clone_case_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++14"],
    deps = DEPS,
)

cc_test(
    name = "datastore_concurrency_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_concurrency_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++14"],
    deps = DEPS,
)

cc_test(
    name = "datastore_snapshot_case_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_snapshot_case_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++14"],
    deps = DEPS,
)

cc_test(
    name = "datastore_stress_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_stress_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++14"],
    deps = DEPS,
)

cc_test(
    name = "datastore_basic_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_basic_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++14"],
    deps = DEPS,
)

cc_test(
    name = "datastore_exception_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_exception_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++14"],
    deps = DEPS,
)

cc_test(
    name = "datastore_restart_test",
    srcs = glob([
        "datastore_integration_base.h",
        "datastore_restart_test.cpp",
        "datastore_integration_main.cpp",
    ]),
    includes = ([]),
    copts = ["-std=c++14"],
    deps = DEPS,
)

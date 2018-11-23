load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

cc_binary(
    name = "topology_dummy_server",
    srcs = [
        "mock_topology.h",
        "topology_dummy_server.cpp",
    ],
    copts = GCC_TEST_FLAGS,
    deps = [
        "//external:gtest",
        "//src/mds/topology",
    ],
)

cc_binary(
    name = "topology_dummy_client",
    srcs = ["topology_dummy_client.cpp"],
    copts = GCC_TEST_FLAGS,
    deps = ["//src/mds/topology"],
)

cc_test(
    name = "topology_utest",
    srcs = [
        "mock_topology.h",
        "test_topology.cpp",
        "test_topology_admin.cpp",
        "test_topology_item.cpp",
        "test_topology_service.cpp",
        "test_topology_service_manager.cpp",
        "test_topology_storage.cpp",
        "topology_test_main.cpp",
    ],
    copts = GCC_TEST_FLAGS,
    deps = [
        "//external:gtest",
        "//src/mds/topology",
    ],
)

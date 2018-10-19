load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS"
    )

cc_binary(
        name = "topology_dummy_server",
        srcs = ["topology_dummy_server.cpp"],
        deps = ["//src/mds/topology:topology"],
        copts = GCC_TEST_FLAGS
        )


cc_binary(
        name = "topology_dummy_client",
        srcs = ["topology_dummy_client.cpp"],
        deps = ["//src/mds/topology:topology"],
        copts = GCC_TEST_FLAGS
        )


cc_test(
        name = "topology_utest",
        srcs = ["topology_test_main.cpp", 
                "test_topology_service_manager.cpp",
                "mock_topology.h",
                "test_topology.cpp",
                "test_topology_storage.cpp",
                "test_topology_service.cpp"],
        deps = ["//external:gtest", "//src/mds/topology:topology"],
        copts = GCC_TEST_FLAGS
        )

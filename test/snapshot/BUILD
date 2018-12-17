load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS"
    )

cc_test(
        name = "snapshot_server_utest",
        srcs = glob([
            "*.cpp","*.h"
        ],
        exclude = ["snapshot_dummy_server.cpp",
                   "fake_curvefs_client.cpp",
                   "fake_curvefs_client.h",
                   "fake_snapshot_server.h",
                   "fake_snapshot_server.cpp",
                   ],
        ),
        deps = ["//test/utils:test-utils", "//external:gtest", "//src/snapshot:snapshot_server_lib"],
        copts = GCC_TEST_FLAGS
        )


#cc_binary(        
#        name = "snapshot_dummy_server",
#        srcs = [
#            "snapshot_dummy_server.cpp",
#            "fake_curvefs_client.cpp",
#            "fake_curvefs_client.h",
#            "fake_snapshot_server.h",
#            "fake_snapshot_server.cpp",
#        ],
#        deps = ["//src/snapshot:snapshot_server_lib"],
#        copts = GCC_TEST_FLAGS
#)

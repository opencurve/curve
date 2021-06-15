load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

COPTS = [
    # "-g",
    # "-O0",
    "-Wall",
    "-Werror",
    # "-fsanitize=address",
    # "-fno-omit-frame-pointer",
    "-std=c++11"
] + select({
    "//curvefs/src/space_allocator:absl_btree": ["-DUSE_BTREE"],
    "//conditions:default": [],
})

LINKOPTS = [
    # "-fsanitize=address",
]

cc_test(
    name = "extents_test",
    srcs = ["extents_test.cpp"],
    deps = [
        "//curvefs/src/space_allocator:curve_fs_space_allocator",
        "@com_google_googletest//:gtest"
    ],
    copts = COPTS,
    linkopts = LINKOPTS
)

cc_test(
    name = "bitmap_allocator_test",
    srcs = [
        "bitmap_allocator_test.cpp",
        "common.h"
    ],
    deps = [
        "//curvefs/src/space_allocator:curve_fs_space_allocator",
        "@com_google_googletest//:gtest"
    ],
    copts = COPTS,
    linkopts = LINKOPTS
)

cc_test(
    name = "bitmap_allocator_brute_test",
    srcs = [
        "bitmap_allocator_brute_test.cpp",
        "common.h"
    ],
    deps = [
        "//curvefs/src/space_allocator:curve_fs_space_allocator",
        "@com_google_googletest//:gtest"
    ],
    copts = COPTS,
    linkopts = LINKOPTS
)

cc_test(
    name = "bitmap_allocator_multi_thread_brute_test",
    srcs = [
        "bitmap_allocator_multi_thread_brute_test.cpp",
        "common.h"
    ],
    deps = [
        "//curvefs/src/space_allocator:curve_fs_space_allocator",
        "@com_google_googletest//:gtest",
    ],
    copts = COPTS,
    linkopts = LINKOPTS
)

cc_test(
    name = "space_alloc_service_test",
    srcs = [
        "space_alloc_service_test.cpp",
        "common.h",
    ],
    deps = [
        "//curvefs/src/space_allocator:curve_fs_space_allocator",
        "//curvefs/test/space_allocator/mock:mock_space_allocator",
        "@com_google_googletest//:gtest",
    ],
    copts = COPTS,
    linkopts = LINKOPTS
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
    name = "fake_user",
    srcs = [
        "fake_user.cpp"
    ],
    copts = COPTS,
    deps = [
        "//curvefs/src/space_allocator:curve_fs_space_allocator",
        "//curvefs/proto:space_cc_proto"
    ],
)

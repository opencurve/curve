load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_SSE42",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-std=c++11",
    "-Wall",
    "-Werror",
]

LINKOPTS = [
]

config_setting(
    name = "absl_btree",
    values = {
        "define": "use_absl_btree=true",
    },
    visibility = ["//visibility:public"],
)

config_setting(
    name = "absl_vector",
    values = {
        "define": "use_absl_vector=true",
    },
    visibility = ["//visibility:public"],
)

ABSL_COPTS = select({
    ":absl_btree": ["-DUSE_ABSL_BTREE"],
    "//conditions:default": [],
}) + select({
    ":absl_vector": ["-DUSE_ABSL_VECTOR"],
    "//conditions:default": [],
})

ABSL_DEPS = select({
    ":absl_btree": ["@com_google_absl//absl/container:btree"],
    "//conditions:default": [],
}) + select({
    ":absl_vector": ["@com_google_absl//absl/container:inlined_vector"],
    "//conditions:default": [],
})

cc_test(
    name = "extents_test",
    srcs = ["extents_test.cpp"],
    copts = COPTS + ABSL_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/space_allocator:curve_fs_space_allocator",
        "@com_google_googletest//:gtest",
    ] + ABSL_DEPS,
)

cc_test(
    name = "bitmap_allocator_test",
    srcs = [
        "bitmap_allocator_test.cpp",
        "common.h",
    ],
    copts = COPTS + ABSL_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/space_allocator:curve_fs_space_allocator",
        "@com_google_googletest//:gtest",
    ] + ABSL_DEPS,
)

cc_test(
    name = "bitmap_allocator_brute_test",
    srcs = [
        "bitmap_allocator_brute_test.cpp",
        "common.h",
    ],
    copts = COPTS + ABSL_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/space_allocator:curve_fs_space_allocator",
        "@com_google_googletest//:gtest",
    ] + ABSL_DEPS,
)

cc_test(
    name = "bitmap_allocator_multi_thread_brute_test",
    srcs = [
        "bitmap_allocator_multi_thread_brute_test.cpp",
        "common.h",
    ],
    copts = COPTS + ABSL_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/space_allocator:curve_fs_space_allocator",
        "@com_google_googletest//:gtest",
    ] + ABSL_DEPS,
)

cc_test(
    name = "space_alloc_service_test",
    srcs = [
        "common.h",
        "space_alloc_service_test.cpp",
    ],
    copts = COPTS + ABSL_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/space_allocator:curve_fs_space_allocator",
        "//curvefs/test/space_allocator/mock:mock_space_allocator",
        "@com_google_googletest//:gtest",
    ] + ABSL_DEPS,
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
    name = "fake_user",
    srcs = [
        "fake_user.cpp",
    ],
    copts = COPTS + ABSL_COPTS,
    deps = [
        "//curvefs/proto:space_cc_proto",
        "//curvefs/src/space_allocator:curve_fs_space_allocator",
    ] + ABSL_DEPS,
)

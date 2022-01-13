#
#  Copyright (c) 2021 NetEase Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")
load("//:copts.bzl", "CURVE_TEST_COPTS")

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
    copts = CURVE_TEST_COPTS + ABSL_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/space:curve_fs_space",
        "@com_google_googletest//:gtest",
    ] + ABSL_DEPS,
)

cc_test(
    name = "bitmap_allocator_test",
    srcs = [
        "bitmap_allocator_test.cpp",
        "common.h",
    ],
    copts = CURVE_TEST_COPTS + ABSL_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/space:curve_fs_space",
        "@com_google_googletest//:gtest",
    ] + ABSL_DEPS,
)

cc_test(
    name = "bitmap_allocator_brute_test",
    srcs = [
        "bitmap_allocator_brute_test.cpp",
        "common.h",
    ],
    copts = CURVE_TEST_COPTS + ABSL_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/space:curve_fs_space",
        "@com_google_googletest//:gtest",
    ] + ABSL_DEPS,
)

cc_test(
    name = "bitmap_allocator_multi_thread_brute_test",
    srcs = [
        "bitmap_allocator_multi_thread_brute_test.cpp",
        "common.h",
    ],
    copts = CURVE_TEST_COPTS + ABSL_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/space:curve_fs_space",
        "@com_google_googletest//:gtest",
    ] + ABSL_DEPS,
)

cc_test(
    name = "space_service_test",
    srcs = [
        "common.h",
        "space_service_test.cpp",
    ],
    copts = CURVE_TEST_COPTS + ABSL_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/space:curve_fs_space",
        "//curvefs/test/space/mock:space_test_mock",
        "@com_google_googletest//:gtest",
    ] + ABSL_DEPS,
)

cc_test(
    name = "space_manager_test",
    srcs = [
        "common.h",
        "space_manager_test.cpp",
    ],
    copts = CURVE_TEST_COPTS + ABSL_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/space:curve_fs_space",
        "//curvefs/test/space/mock:space_test_mock",
        "@com_google_googletest//:gtest",
    ] + ABSL_DEPS,
)

cc_test(
    name = "allocator_test",
    srcs = [
        "allocator_test.cpp",
        "common.h",
    ],
    copts = CURVE_TEST_COPTS + ABSL_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/space:curve_fs_space",
        "@com_google_googletest//:gtest",
    ] + ABSL_DEPS,
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_binary
cc_binary(
    name = "fake_user",
    srcs = [
        "fake_user.cpp",
    ],
    copts = CURVE_TEST_COPTS + ABSL_COPTS,
    deps = [
        "//curvefs/proto:space_cc_proto",
        "//curvefs/src/space:curve_fs_space",
    ] + ABSL_DEPS,
)

#
#  Copyright (c) 2022 NetEase Inc.
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

load("//:copts.bzl", "CURVE_TEST_COPTS")

LINKOPTS = [
    "-pthread",
]

cc_test(
    name = "volume_unittest",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = CURVE_TEST_COPTS,
    linkopts = LINKOPTS,
    deps = [
        "//curvefs/src/volume",
        "//curvefs/test/client/rpcclient:rpcclient_test_mock",
        "//curvefs/test/volume/mock",
        "//test/client/mock:client_mock_lib",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)

# cc_test(
#     name = "extents_test",
#     srcs = ["extents_test.cpp"],
#     copts = CURVE_TEST_COPTS,
#     linkopts = LINKOPTS,
#     deps = [
#         "//curvefs/src/volume",
#         "@com_google_googletest//:gtest",
#         "@com_google_googletest//:gtest_main",
#     ],
# )

# cc_test(
#     name = "bitmap_allocator_test",
#     srcs = [
#         "bitmap_allocator_test.cpp",
#         "common.h",
#     ],
#     copts = CURVE_TEST_COPTS,
#     linkopts = LINKOPTS,
#     deps = [
#         "//curvefs/src/volume",
#         "@com_google_googletest//:gtest",
#         "@com_google_googletest//:gtest_main",
#     ],
# )

# cc_test(
#     name = "bitmap_allocator_brute_test",
#     srcs = [
#         "bitmap_allocator_brute_test.cpp",
#         "common.h",
#     ],
#     copts = CURVE_TEST_COPTS,
#     linkopts = LINKOPTS,
#     deps = [
#         "//curvefs/src/volume",
#         "@com_google_googletest//:gtest",
#         "@com_google_googletest//:gtest_main",
#     ],
# )

# cc_test(
#     name = "bitmap_allocator_multi_thread_brute_test",
#     srcs = [
#         "bitmap_allocator_multi_thread_brute_test.cpp",
#         "common.h",
#     ],
#     copts = CURVE_TEST_COPTS,
#     linkopts = LINKOPTS,
#     deps = [
#         "//curvefs/src/volume",
#         "@com_google_googletest//:gtest",
#         "@com_google_googletest//:gtest_main",
#     ],
# )

# cc_test(
#     name = "block_device_test",
#     srcs = [
#         "block_device_client_test.cpp",
#     ],
#     copts = CURVE_TEST_COPTS,
#     deps = [
#         "//curvefs/src/volume",
#         "//curvefs/test/volume/mock",
#         "//test/client/mock:client_mock_lib",
#         "@com_google_googletest//:gtest",
#         "@com_google_googletest//:gtest_main",
#     ],
# )

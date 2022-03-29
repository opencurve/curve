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

load("//:copts.bzl", "CURVE_TEST_COPTS")

cc_test(
    name = "block_device_client_test",
    srcs = glob([
        "block_device_client_test.cpp"
        ]),
    copts = CURVE_TEST_COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//include/client:include_client",
        "//curvefs/src/client:fuse_client_lib",
        "//test/client/mock:client_mock_lib",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
cc_library(
    name = "client_s3_test_lib",
    srcs = glob([
        "mock_client_s3.h",
        "mock_metaserver_service.h",
        "mock_spacealloc_service.h",
    ]),
    copts = CURVE_TEST_COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:gtest",
         "//curvefs/src/client:fuse_client_lib",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/proto:space_cc_proto",
    ],
    linkopts = ["-L/usr/local/lib/x86_64-linux-gnu"],
)

cc_binary(
    name = "client_s3_test",
    srcs = glob([
        "main.cpp",
        "client_s3_adaptor_test.cpp",
        "client_s3_adaptor_test2.cpp",
        "client_s3_test.cpp",
        "*.h",
    ]),
    copts = CURVE_TEST_COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//external:gtest",
         "//curvefs/src/client:fuse_client_lib",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/proto:space_cc_proto",
        "//curvefs/test/client/rpcclient:rpcclient_test_mock",
        "//test/client/mock:client_mock_lib",
    ],
    linkopts = [
                "-L/usr/local/lib/x86_64-linux-gnu"],
)

cc_test(
   name = "curve_fuse_test",
   srcs = glob([
       "*.cpp",
       "*.c",
       "*.h"],
       exclude = [ "block_device_client_test.cpp",
                   "client_s3_adaptor_test.cpp",
                   "client_s3_adaptor_test2.cpp",
                   "client_s3_test.cpp" ]
   ),
   copts = CURVE_TEST_COPTS + ["-I/usr/local/include/fuse3"],
   deps = [
        "//external:gtest",
        "@com_google_googletest//:gtest_main",
        "//include/client:include_client",
        "//curvefs/src/client:fuse_client_lib",
        "//test/client/mock:client_mock_lib",
        "//curvefs/test/client/rpcclient:rpcclient_test_mock",
        "//curvefs/proto:metaserver_cc_proto",
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/proto:space_cc_proto",
   ],
   linkopts = ["-lfuse3",
               "-L/usr/local/lib/x86_64-linux-gnu"],
   visibility = ["//visibility:public"],
)

cc_test(
    name = "curvefs_fs_cache_manager_test",
    srcs = [
        "fs_cache_manager_test.cpp",
    ] + glob([
        "*.h",
    ]),
    deps = [
        "//curvefs/src/client:fuse_client_lib",
        "//external:gtest",
        "@com_google_googletest//:gtest_main",
    ],
    copts = CURVE_TEST_COPTS,
)

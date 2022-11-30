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

cc_library(
    name = "client_test_utils",
    srcs = [
        "utils.cpp",
    ],
    hdrs = [
        "utils.h",
    ],
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:metaserver_cc_proto",
    ],
)

cc_test(
    name = "client_s3_test",
    srcs = glob([
        "main.cpp",
        "client_s3_adaptor_test.cpp",
        "fs_cache_manager_test.cpp",
        "file_cache_manager_test.cpp",
        "chunk_cache_manager_test.cpp",
        "data_cache_test.cpp",
        "client_s3_test.cpp",
        "client_s3_adaptor_Integration.cpp",
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
                   "client_s3_test.cpp",
                   "client_s3_adaptor_test.cpp",
                   "fs_cache_manager_test.cpp",
                   "file_cache_manager_test.cpp",
                   "chunk_cache_manager_test.cpp",
                   "data_cache_test.cpp",
                   "client_s3_adaptor_Integration.cpp",
                   "client_memcache_test.cpp",
                 ],
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
        "//curvefs/test/volume/mock",
   ],
   linkopts = ["-lfuse3",
               "-L/usr/local/lib/x86_64-linux-gnu"],
   visibility = ["//visibility:public"],
)

cc_library(
    name = "mock",
    hdrs = glob([
        "mock_*.h"
    ]),
    deps = [
        "//curvefs/src/client:fuse_client_lib",
        "@com_google_googletest//:gtest",
    ],
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
)

cc_test(
    name = "curvefs_client_memcache_test",
    srcs = [
        "client_memcache_test.cpp",
    ],
    deps = [
        "//curvefs/src/client/kvclient:memcached_client_lib",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
        "//external:gtest",
        "//external:glog",
    ],
    linkopts = ["-lmemcached"],
    copts = CURVE_TEST_COPTS,
    visibility = ["//visibility:public"],
)

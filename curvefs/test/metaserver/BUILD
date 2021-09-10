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

load(
    "//:copts.bzl",
    "GCC_FLAGS",
    "GCC_TEST_FLAGS",
)

COPTS = [
    "-DGFLAGS=gflags",
    "-DOS_LINUX",
    "-DSNAPPY",
    "-DHAVE_SSE42",
    "-DNDEBUG",
    "-fno-omit-frame-pointer",
    "-momit-leaf-frame-pointer",
    "-msse4.2",
    "-pthread",
    "-Wsign-compare",
    "-Wno-unused-parameter",
    "-Wno-unused-variable",
    "-Woverloaded-virtual",
    "-Wnon-virtual-dtor",
    "-Wno-missing-field-initializers",
    "-std=c++11",
]

cc_test(
    name = "curvefs_metaserver_test",
    srcs = glob(
        ["*.cpp", "*.h"],
        exclude = [
            "dumpfile_test.cpp",
            "storage_test.cpp",
            "metastore_test.cpp",
            "mock_client_s3.h",
            "mock_metaserver_s3.h",
            "metaserver_s3_adaptor_test.h",
            "metaserver_s3_adaptor_test.cpp",
            "mock_metaserver_s3_adaptor.h",
            "metaserver_s3_test.cpp"
        ],
    ),
    copts = ["-g","-DHAVE_ZLIB=1"],
    deps = [
            "//curvefs/src/metaserver:curvefs_metaserver_lib",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            "//external:brpc",
    ],
)

# s3 adaptor
cc_test(
    name = "metaserver_s3_adaptor_test",
    srcs = glob([
        "mock_metaserver_s3.h",
        "metaserver_s3_adaptor_test.h",
        "metaserver_s3_adaptor_test.cpp",
        "curvefs/test/client/mock_client_s3.h",
        "mock_metaserver_s3_adaptor.h",
        "metaserver_s3_test.cpp"
    ]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
        "//external:brpc",
        "//external:glog",
        "//external:gflags",
        "//include/client:include_client",
        "//src/common:curve_s3_adapter",
        "//src/client:curve_client",
        "//curvefs/src/client:fuse_client_lib",
        "//curvefs/src/metaserver:metaserver_s3_lib",
        "//curvefs/test/client:client_s3_test_lib",
        "//test/client/mock:client_mock_lib",
        "//test/common:common_mock"
    ],
    linkopts = [
        "-lfuse3",
        "-lpthread",
        "-L/usr/local/lib/x86_64-linux-gnu",
    ],
)

cc_test(
    name = "curvefs_metaserver_metastore_test",
    srcs = glob(
        ["metastore*"],
    ),
    copts = ["-g","-DHAVE_ZLIB=1"],
    deps = [
            "//curvefs/src/metaserver:curvefs_metaserver_lib",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            "//external:brpc",
    ],
)

cc_test(
    name = "dumpfile_test",
    srcs = glob([ "dumpfile_test.cpp" ]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/metaserver:curvefs_metaserver_lib",
        "@com_google_googletest//:gtest_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "storage_test",
    srcs = glob([ "storage_test.cpp" ]),
    copts = COPTS,
    defines = ["UNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/metaserver:curvefs_metaserver_lib",
        "@com_google_googletest//:gtest_main",
        "@com_google_googletest//:gtest",
    ],
)

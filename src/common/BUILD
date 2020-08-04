#
#  Copyright (c) 2020 NetEase Inc.
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

cc_library(
    name = "curve_common",
    srcs = glob([
        "*.h",
        "*.cpp",
        ],exclude = ["authenticator.*", "s3_adapter.*"]
    ),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//external:brpc",
        "//external:butil",
        "//src/common/concurrent:curve_concurrent"
    ],
)

cc_library(
    name = "curve_auth",
    srcs = glob([
        "authenticator.h",
        "authenticator.cpp",
    ]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//external:butil",
        "//src/common/concurrent:curve_concurrent"
    ],
)

cc_library(
    name = "curve_uncopy",
    srcs = glob([
        "uncopyable.h",
    ]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
    ],
)

cc_library(
    name = "curve_s3_adapter",
    srcs = glob([
        "s3_adapter.h",
        "s3_adapter.cpp",
    ]),
    copts = COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//external:glog",
        "//src/common:curve_common",
        "@aws",
    ],
)

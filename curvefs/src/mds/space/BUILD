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

load("//:copts.bzl", "CURVE_DEFAULT_COPTS")

cc_library(
    name = "curvefs_mds_space",
    srcs = glob([
        "*.cpp",
        "*.h",
    ]),
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:mds_cc_proto",
        "//curvefs/proto:space_cc_proto",
        "//curvefs/src/mds:curvefs_mds_fs_storage",
        "//curvefs/src/mds/codec:fs_mds_codec",
        "//external:brpc",
        "//include/client:include_client",
        "//src/client:curve_client",
        "//src/common:curve_common",
        "//src/kvstorageclient:kvstorage_client",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/memory",
        "@com_google_googletest//:gtest_prod",
    ],
)

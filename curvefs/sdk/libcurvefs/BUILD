#
#  Copyright (c) 2023 NetEase Inc.
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

cc_binary(
    name = "curvefs",
    srcs = [
        "libcurvefs.h",
        "libcurvefs.cpp",
    ],
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/client/vfs:vfs",
    ],
    linkshared = True,
)

cc_library(
    name = "curvefs_lib",
    srcs = [
        "libcurvefs.h",
        "libcurvefs.cpp",
    ],
    copts = CURVE_DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/src/client/vfs:vfs",
    ],
)

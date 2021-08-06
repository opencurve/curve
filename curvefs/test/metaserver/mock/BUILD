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
    "-std=c++11",
    "-Wall",
    "-Werror",
]

cc_library(
    name = "metaserver_test_mock",
    hdrs = glob(["*.h"]),
    copts = COPTS + ["-DUNIT_TEST"],
    visibility = ["//visibility:public"],
    deps = [
        "//curvefs/proto:cli2_cc_proto",
        "//curvefs/src/metaserver:curvefs_metastore",
    ],
)

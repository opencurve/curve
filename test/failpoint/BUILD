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

cc_test(
    name = "failpoint_test",
    srcs = glob(["*.cpp", "*.h"]),
    copts = ["-g","-DHAVE_ZLIB=1", "-DFIU_ENABLE=1", "-DFIU_TRACE=1"],
    deps = [
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
    ],
    linkopts = ["-lfiu"],
)

# https://docs.bazel.build/versions/master/be/c-cpp.html#cc_library
# cc_library(
#     name = "fakes",
#     srcs = [],
#     hdrs = ["fakes.h"],
#     deps = [
#         "//src/mds/topology:topology",
#          "//src/mds/nameserver2:nameserver2",
#     ],
#     visibility = ["//visibility:public"],
# )

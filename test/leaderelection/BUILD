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
    name = "leader_election_test",
    srcs = glob(["*.cpp", "*.h"]),
    copts = ["-g","-DHAVE_ZLIB=1"],
    defines = ["FIU_ENABLE"],
    deps = [
            "//src/leader_election:leader_election_for_test",
            "//src/mds/nameserver2/helper",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            "//test/mds/mock:common_mock",
    ],
    linkopts = ["-lfiu"],
)
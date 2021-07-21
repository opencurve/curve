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
    name = "id_generator_test",
    srcs = glob(["*.cpp", "*.h"]),
    copts = ["-g","-DHAVE_ZLIB=1"],
    deps = [
            "//src/mds/nameserver2:nameserver2",
            "@com_google_googletest//:gtest_main",
            "@com_google_googletest//:gtest",
            "//src/common:curve_common",
            "//src/mds/nameserver2/helper:helper",
            "//src/mds/nameserver2/idgenerator:nameserver_idgenerator",
            "//test/mds/mock:common_mock",
            "//test/mds/nameserver2/mock:nameserver2_mock",
    ],
)
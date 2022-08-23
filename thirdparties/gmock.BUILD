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

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "gtest_prod",
    hdrs = ["googletest/include/gtest/gtest_prod.h"],
    includes = ["googletest/include"],
)

cc_library(
      name = "gtest",
      srcs = [
            "googletest/src/gtest-all.cc",
            "googlemock/src/gmock-all.cc",
      ],
      hdrs = glob([
          "**/*.h",
          "googletest/src/*.cc",
          "googlemock/src/*.cc",
      ]),
      includes = [
          "googlemock",
          "googletest",
          "googletest/include",
          "googlemock/include",
      ],
      linkopts = ["-pthread"],
  )

cc_library(
      name = "gtest_main",
      srcs = ["googlemock/src/gmock_main.cc"],
      linkopts = ["-pthread"],
      deps = [":gtest"],
)
